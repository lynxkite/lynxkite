package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite

class TeradataJdbcDialectTest extends FunSuite {

  test("test TeradataDialect") {
    val magicMarker = "--LYNX-TD-SCHEMA-AUTO-FIX"

    val dialect = new TeradataDialect()

    // This is testing Spark's behavior. Needed for reference for below tests.
    assert("SELECT * FROM table WHERE 1=0" == dialect.getSchemaQuery("table"))

    // For a table, autofix should just remove the magic marker
    // and fall back to Spark's logic.
    assert("SELECT * FROM table WHERE 1=0" ==
      dialect.getSchemaQuery("table" + magicMarker))
    // Handle query without WHERE clause:
    assert("SeLeCt * FROM table2 WHERE 1=0" ==
      dialect.getSchemaQuery("SeLeCt * FROM table2" + magicMarker))
    // Handle query with WHERE clause and JOIN:
    assert("SeLeCT a.*, b.b FROM a INNER JOIN b ON a.x = b.y WHERE 1=0" ==
      dialect.getSchemaQuery(
        "SeLeCT a.*, b.b FROM a INNER JOIN b ON a.x = b.y WhErE a.a = 1 AND a.b = 2" + magicMarker))

    intercept[AssertionError] {
      dialect.getSchemaQuery("select where where " + magicMarker)
    }

  }
}
