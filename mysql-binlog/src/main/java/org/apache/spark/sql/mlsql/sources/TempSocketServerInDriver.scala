package org.apache.spark.sql.mlsql.sources

import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging
import org.apache.spark.sql.mlsql.sources.mysql.binlog.{BinLogSocketServerSerDer, ReportBinlogSocketServerHostAndPort, SocketServerInExecutor}

/**
 * driver端启动的serverSocket线程：用于接收executor端启动binlogServer后的ip、port
 * @author jian.huang
 * @version 4.2
 * 2020-12-21 21:24:32
 */
class TempSocketServerInDriver(context: AtomicReference[ReportBinlogSocketServerHostAndPort]) extends BinLogSocketServerSerDer with Logging {
  val (server, host, port) = SocketServerInExecutor.setupOneConnectionServer("driver-socket-server") { sock =>
    handleConnection(sock)
  }

  def handleConnection(socket: Socket): Unit = {
    val dIn = new DataInputStream(socket.getInputStream)
    val req = readRequest(dIn).asInstanceOf[ReportBinlogSocketServerHostAndPort]
    context.set(req)
  }
}
