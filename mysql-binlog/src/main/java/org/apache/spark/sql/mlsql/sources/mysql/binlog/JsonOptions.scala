package org.apache.spark.sql.mlsql.sources.mysql.binlog

import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.ParseModes.FAIL_FAST_MODE

/**
 * 22/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
object JsonOptions {
  def options(timeZone: String) = {
    new JSONOptions(Map() + ("mode" -> FAIL_FAST_MODE))
  }
}
