package com.lynxanalytics.biggraph.spark_util

import org.apache.spark.sql.jdbc.JdbcDialect

// Teradata sometimes "forgets" the schema of the result of a
// JDBC query, see issue #5631.
// This dialect is able to correct that in case of simple
// queries. This logic can be enabled via appending
// /*LYNX-TD-SCHEMA-AUTO-FIX*/ to the end of queries.
class TeradataDialect extends JdbcDialect {
  val magicMarker = "/*LYNX-TD-SCHEMA-AUTO-FIX*/"
  def canHandle(url: String) = {
    url.startsWith("jdbc:teradata:")
  }

  def unpackMarkedQuery(table: String): Option[String] = {
    if (table.endsWith(magicMarker)) {
      Some(table.substring(0, table.size - magicMarker.size))
    } else {
      None
    }
  }

  // Removes the where clause of query if exists, and asserts that there
  // was at most one where clause in the query.
  def clearWhereClause(query: String): String = {
    val whereMatcher = "(?i)\\bwhere\\b".r
    val matches = whereMatcher.findAllMatchIn(query).toList
    assert(
      matches.size <= 1,
      s"Multiple WHERE clauses in Teradata query, autofix failed: $query")
    if (matches.size == 1) {
      // Query contains WHERE clause
      matches(0).before.toString
    } else {
      // query contains no where clause
      query
    }
  }

  override def getSchemaQuery(table: String) = {
    unpackMarkedQuery(table) match {
      case Some(query) =>
        // Magic marker is present.
        if (query.startsWith("(")) {
          val lastBracket = query.lastIndexOf(")")
          val cleanedPrefix = clearWhereClause(
            query.substring(1, lastBracket))
          cleanedPrefix + " WHERE 1=0"
        } else {
          super.getSchemaQuery(query)
        }
      case None =>
        // Magic marker is not present.
        super.getSchemaQuery(table)
    }
  }
}
