// JDBC related utilities.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

import anorm.SQL
import java.sql

object JDBCQuoting {
  private val SimpleIdentifier = "[a-zA-Z0-9_]+".r
  def quoteIdentifier(s: String) = {
    s match {
      case SimpleIdentifier() => s
      case _ => '"' + s.replaceAll("\"", "\"\"") + '"'
    }
  }
}

case class TableStats(table: String, keyColumn: String)(implicit val connection: sql.Connection) {
  val quotedTable = JDBCQuoting.quoteIdentifier(table)
  val quotedKey = JDBCQuoting.quoteIdentifier(keyColumn)
  val (minKey, maxKey) = {
    val query = s"SELECT MIN($quotedKey) AS min, MAX($quotedKey) AS max FROM $quotedTable"
    log.info(s"Executing query: $query")
    val q = SQL(query)
    q().map(r => (r[Long]("min"), r[Long]("max"))).head
  }
  val count = {
    val query = s"SELECT COUNT(*) AS count FROM $quotedTable"
    log.info(s"Executing query: $query")
    val q = SQL(query)
    q().map(r => r[Long]("count")).head
  }
}
