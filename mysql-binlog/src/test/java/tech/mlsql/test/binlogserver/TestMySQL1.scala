package tech.mlsql.test.binlogserver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.scalatest.FunSuite

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class TestMySQL1 extends FunSuite {

  val tableName = "t1"
  test("mysql") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MySQL B Sync")
      .getOrCreate()

    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
      option("host", "172.24.5.212").
      option("port", "3306").
      option("userName", "root").
      option("password", "root").
      option("databaseNamePattern", "test").
      option("tableNamePattern", tableName).
      //      option("bingLogNamePrefix", "mysql-bin").
      //      option("binlogIndex", "16").
      //      option("binlogFileOffset", "3869").
      option("binlog.field.decode.first_name", "UTF-8").
      load()

    //    val query = df.writeStream.
    //      format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
    //      option("__path__", "/tmp/datalake/{db}/{table}").
    //      option("path", "{db}/{table}").
    //      option("mode", "Append").
    //      option("idCols", "id").
    //      option("duration", "3").
    //      option("syncType", "binlog").
    //      option("checkpointLocation", "/tmp/cpl-binlog4").
    //      outputMode("append")
    //      .trigger(Trigger.ProcessingTime("15 seconds"))
    //      .start()
    val query = df.writeStream.
      format("console").
      option("mode", "Append").
      option("truncate", "false").
      option("numRows", "100000").
      option("checkpointLocation", "/tmp/cpl-mysql6").
      outputMode("append")
      .trigger(ProcessingTime("3 seconds"))
      .start()

    query.awaitTermination()
  }
}

