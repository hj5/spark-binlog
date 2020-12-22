package org.apache.spark.sql.mlsql.sources.mysql.binlog

import java.io.{DataInputStream, DataOutputStream}
import java.net.{InetAddress, ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.mlsql.sources.ExecutorInternalBinlogConsumer
import org.apache.spark.{SparkEnv, SparkException}
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.common.utils.serder.json.JsonUtils


object SocketServerInExecutor extends Logging {

  private val binlogServerHolder = new java.util.concurrent.ConcurrentHashMap[MySQLBinlogServerInfo, BinLogSocketServerInExecutor[_]]()
  val threadPool = Executors.newFixedThreadPool(100)

  def addNewBinlogServer(a: MySQLBinlogServerInfo, b: BinLogSocketServerInExecutor[_]) = {
    binlogServerHolder.put(a, b)
  }

  def setupOneConnectionServer(threadName: String)
                              (func: Socket => Unit): (ServerSocket, String, Int) = {

    val host = if (SparkEnv.get == null) {
      //When SparkEnv.get is null, the program may run in a test
      //So return local address would be ok.
      "127.0.0.1"
    } else {
      if (SparkEnv.get.rpcEnv.address != null) {
        SparkEnv.get.rpcEnv.address.host
      } else {
        val hostName = tech.mlsql.common.utils.network.SparkExecutorInfo.getInstance.hostname
        if (hostName == null) NetUtils.getHost else hostName
      }
    }

    val serverSocket: ServerSocket = new ServerSocket(0, 1, InetAddress.getByName(host))
    // Close the socket if no connection in 5 min
    serverSocket.setSoTimeout(1000 * 60 * 5)
    new Thread(threadName) {
      setDaemon(true)

      override def run(): Unit = {
        var sock: Socket = null
        try {
          sock = serverSocket.accept()
          func(sock)
        } finally {
          JavaUtils.closeQuietly(serverSocket)
          JavaUtils.closeQuietly(sock)
        }
      }
    }.start()

    (serverSocket, host, serverSocket.getLocalPort)
  }


  def setupMultiConnectionServer[T](taskContextRef: AtomicReference[T], threadName: String)
                                   (func: Socket => Unit)(completeCallback: () => Unit): (ServerSocket, String, Int) = {


    val host = if (SparkEnv.get == null) {
      //When SparkEnv.get is null, the program may run in a test
      //So return local address would be ok.
      "127.0.0.1"
    } else {
      if (SparkEnv.get.rpcEnv.address != null) {
        SparkEnv.get.rpcEnv.address.host
      } else {
        val hostName = tech.mlsql.common.utils.network.SparkExecutorInfo.getInstance.hostname
        if (hostName == null) NetUtils.getHost else hostName
      }

    }
    val serverSocket: ServerSocket = new ServerSocket(0, 1, InetAddress.getByName(host))
    // throw exception if  the socket server have no connection in 5 min
    // then we will close the serverSocket
    //serverSocket.setSoTimeout(1000 * 60 * 5)

    new Thread(threadName) {
      setDaemon(true)

      override def run(): Unit = {
        try {
          /**
            * Since we will start this BinLogSocketServerInExecutor in spark task, so when we kill the task,
            * The taskContext should also be null
            */
          while (taskContextRef.get() != null) {
            val socket = serverSocket.accept()
            threadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Received connection from" + socket)
                  func(socket)
                } catch {
                  case e: Exception =>
                    logError(s"The server ${serverSocket} is closing the socket ${socket} connection", e)
                } finally {
                  JavaUtils.closeQuietly(socket)
                }
              }
            })
          }
          completeCallback()
          JavaUtils.closeQuietly(serverSocket)
        }
        catch {
          case e: Exception => logError("", e)
        }

      }
    }.start()

    (serverSocket, host, serverSocket.getLocalPort)
  }
}

abstract class SocketServerInExecutor[T](taskContextRef: AtomicReference[T], threadName: String) {

  val (server, host, port) = SocketServerInExecutor.setupMultiConnectionServer(taskContextRef, threadName) { sock =>
    handleConnection(sock)
  }(() => {
    close
  })

  /**
   * binlogServer serverSocket的主体代码（线程的启动在servers.scala中）
   * 由spark source接口方法触发，方法体会将binlogServer搜集的所有binlog变化json（由binLog变化事件转化而来）
   * 发送给spark source接口方法，供其生成dataframe、offset
   * clientSocket入口：{@link BinLogSocketServerSerDer.sendRequest }
   * @param socket
   */
  def handleConnection(socket: Socket): Unit

  def close: Unit
}


trait BinLogSocketServerSerDer {
  def readRequest(dIn: DataInputStream) = {
    val length = dIn.readInt()
    val bytes = new Array[Byte](length)
    dIn.readFully(bytes, 0, length)
    val response = JsonUtils.fromJson[BinlogSocketRequest](new String(bytes, StandardCharsets.UTF_8)).unwrap
    response
  }

  /**
   * MLSQLBinLogSource向binlogServer获取数据的请求发送出口（在spark source的接口方法中被调用）
   * @param dOut
   * @param request
   */
  def sendRequest(dOut: DataOutputStream, request: Request) = {
    val bytes = request.json.getBytes(StandardCharsets.UTF_8)
    dOut.writeInt(bytes.length)
    dOut.write(bytes)
    dOut.flush()
  }

  def sendResponse(dOut: DataOutputStream, response: Response) = {
    val bytes = response.json.getBytes(StandardCharsets.UTF_8)
    dOut.writeInt(bytes.length)
    dOut.write(bytes)
    dOut.flush()
  }

  def readResponse(dIn: DataInputStream) = {
    val length = dIn.readInt()
    val bytes = new Array[Byte](length)
    dIn.readFully(bytes, 0, length)
    val response = JsonUtils.fromJson[BinlogSocketResponse](new String(bytes, StandardCharsets.UTF_8)).unwrap
    response
  }

  /**
   * BinLogSocketServerInExecutor 进行请求响应时，向client发送数据的『开始』、『结束』标识
   * @param dOut
   * @param mark
   */
  def sendMark(dOut: DataOutputStream, mark:Int): Unit = {
    dOut.writeInt(mark)
    dOut.flush()
  }

  /**
   * BinLogSocketServerInExecutor 向client迭代发送真正的binlog数据
   * 是发送『开始』、『结束』标识之间的数据
   * @param dOut
   * @param response
   */
  def iterativeSendData(dOut: DataOutputStream, response: Response) = {
    val bytes = response.json.getBytes(StandardCharsets.UTF_8)
    dOut.writeInt(bytes.length)
    dOut.write(bytes)
    dOut.flush()
  }

  /**
   * 从{@link SocketServerInExecutor.handleConnection}读取封装好的binLog数据
   * @param dIn
   * @return
   */
  def readIteratedResponse(dIn: DataInputStream): Iterator[Response] = {

    new Iterator[Response]() {
      private var dataOrMark = SocketReplyMark.HEAD
      private var nextObj: Response = _
      private var eos = false
      override def hasNext: Boolean = {
        nextObj != null || {
          if ( !eos ) {
            dataOrMark = dIn.readInt
            getNext(dataOrMark)
          } else {
            false
          }
        }
      }

      def getNext(mark : Int) : Boolean = {
        mark match {
          case SocketReplyMark.END =>
            eos = true // eos=true !!!
            false  // 结束位
          case SocketReplyMark.HEAD => // 开始位 需要继续读一位
            dataOrMark = dIn.readInt // 长度位 or end
            getNext(dataOrMark)
          case _ =>
            val bytes = new Array[Byte](dataOrMark)
            dIn.readFully(bytes, 0, dataOrMark)
            val response = JsonUtils.fromJson[BinlogSocketResponse](new String(bytes, StandardCharsets.UTF_8)).unwrap
            nextObj = response
            true
        }
      }

      override def next(): Response = {
        if (hasNext) {
          val obj = nextObj
          nextObj = null.asInstanceOf[Response]
          obj
        } else {
          Iterator.empty.next()
        }
      }
    }
  }

}

object BinLogSocketServerCommand extends BinLogSocketServerSerDer {

}

object ExecutorBinlogServerConsumerCache extends Logging {

  private case class CacheKey(host: String, port: Int)

  private lazy val cache = {
    val conf = SparkEnv.get.conf
    val capacity = conf.getInt("spark.sql.mlsql.binlog.capacity", 64)
    new java.util.LinkedHashMap[CacheKey, ExecutorInternalBinlogConsumer](capacity, 0.75f, true) {
      override def removeEldestEntry(
                                      entry: java.util.Map.Entry[CacheKey, ExecutorInternalBinlogConsumer]): Boolean = {

        // Try to remove the least-used entry if its currently not in use.
        //
        // If you cannot remove it, then the cache will keep growing. In the worst case,
        // the cache will grow to the max number of concurrent tasks that can run in the executor,
        // (that is, number of tasks slots) after which it will never reduce. This is unlikely to
        // be a serious problem because an executor with more than 64 (default) tasks slots is
        // likely running on a beefy machine that can handle a large number of simultaneously
        // active consumers.

        if (!entry.getValue.inUse && this.size > capacity) {
          logWarning(
            s"KafkaConsumer cache hitting max capacity of $capacity, " +
              s"removing consumer for ${entry.getKey}")
          try {
            entry.getValue.close
          } catch {
            case e: SparkException =>
              logError(s"Error closing earliest Kafka consumer for ${entry.getKey}", e)
          }
          true
        } else {
          false
        }
      }
    }
  }

  def acquire(executorBinlogServerInfo: ExecutorBinlogServerInfo): ExecutorInternalBinlogConsumer = synchronized {
    val key = new CacheKey(executorBinlogServerInfo.host, executorBinlogServerInfo.port)
    val existingInternalConsumer = cache.get(key)

    lazy val newInternalConsumer = new ExecutorInternalBinlogConsumer(executorBinlogServerInfo)

    if (existingInternalConsumer == null) {
      // If consumer is not already cached, then put a new in the cache and return it
      cache.put(key, newInternalConsumer)
      newInternalConsumer.inUse = true
      newInternalConsumer

    } else if (existingInternalConsumer.inUse) {
      // If consumer is already cached but is currently in use, then return a new consumer
      newInternalConsumer

    } else {
      // If consumer is already cached and is currently not in use, then return that consumer
      existingInternalConsumer.inUse = true
      existingInternalConsumer
    }
  }

  def release(intConsumer: ExecutorInternalBinlogConsumer): Unit = {
    synchronized {

      // Clear the consumer from the cache if this is indeed the consumer present in the cache
      val key = new CacheKey(intConsumer.executorBinlogServer.host,
        intConsumer.executorBinlogServer.port)

      val cachedIntConsumer = cache.get(key)
      if (intConsumer.eq(cachedIntConsumer)) {
        // The released consumer is the same object as the cached one.
        if (intConsumer.markedForClose) {
          intConsumer.close
          cache.remove(key)
        } else {
          intConsumer.inUse = false
        }
      } else {
        // The released consumer is either not the same one as in the cache, or not in the cache
        // at all. This may happen if the cache was invalidate while this consumer was being used.
        // Just close this consumer.
        intConsumer.close
        logInfo(s"Released a supposedly cached consumer that was not found in the cache")
      }
    }
  }
}
