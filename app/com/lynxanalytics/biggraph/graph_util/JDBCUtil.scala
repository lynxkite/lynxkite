// JDBC related utilities.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

import java.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object JDBCQuoting {
  private val SimpleIdentifier = "[a-zA-Z0-9_]+".r
  def quoteIdentifier(s: String) = {
    s match {
      case SimpleIdentifier() => s
      case _ => '"' + s.replaceAll("\"", "\"\"") + '"'
    }
  }
}

object JDBCUtil {
  def read(context: SQLContext, url: String, table: String, keyColumn: String): DataFrame = {
    assert(url.startsWith("jdbc:"), "JDBC URL has to start with jdbc:")
    val props = new java.util.Properties
    if (keyColumn.isEmpty) {
      // Inefficiently read into a single partition.
      context.read.jdbc(url, table, props)
    } else {
      val stats = try TableStats(url, table, keyColumn)
      val numPartitions = RuntimeContext.partitionerForNRows(stats.count).numPartitions
      stats.keyType match {
        case "String" => ???
        case "Number" =>
          context.read.jdbc(
            url,
            table,
            keyColumn,
            stats.minIntKey.get,
            stats.maxIntKey.get,
            numPartitions,
            props)
      }
    }
  }
}

case class TableStats(
  count: Long, keyType: String,
  minIntKey: Option[Long], maxIntKey: Option[Long],
  minStringKey: Option[String], maxStringKey: Option[String])
object TableStats {
  def apply(url: String, table: String, keyColumn: String): TableStats = {
    val quotedTable = JDBCQuoting.quoteIdentifier(table)
    val quotedKey = JDBCQuoting.quoteIdentifier(keyColumn)
    val query =
      s"SELECT COUNT(*) as count, MIN($quotedKey) AS min, MAX($quotedKey) AS max FROM $quotedTable"
    log.info(s"Executing query: $query")
    val connection = sql.DriverManager.getConnection(url)
    try {
      val statement = connection.prepareStatement(query)
      try {
        val rs = statement.executeQuery()
        try {
          val md = rs.getMetaData
          println(md.getColumnType(1))
          println(md.getColumnTypeName(1))
          new TableStats(rs.getLong("count"), "???", None, None, None, None)
        } finally rs.close()
      } finally statement.close()
    } finally connection.close()
  }
}
