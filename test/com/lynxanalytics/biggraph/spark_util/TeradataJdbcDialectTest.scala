package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite

class TeradataJdbcDialectTest extends FunSuite {

  test("test TeradataDialect") {
    val overrideTable = "--LYNX-TD-SCHEMA-OVERRIDE-TABLE:"
    val overrideQuery = "--LYNX-TD-SCHEMA-OVERRIDE-SQL:"
    val autofix = "--LYNX-TD-SCHEMA-AUTO-FIX"

    val dialect = new TeradataDialect()

    // This is testing Spark's behavior. Needed for reference for below tests.
    assert("SELECT * FROM table WHERE 1=0" == dialect.getSchemaQuery("table"))
    // Schema override is specified with table:
    assert("SELECT * FROM t42 WHERE 1=0" ==
      dialect.getSchemaQuery("table" + overrideTable + "t42"))
    // Schema override is specified with query:
    assert("t42" ==
      dialect.getSchemaQuery("table" + overrideQuery + "t42"))

    // Test our "autofix" mode:
    // Autofix is not active, Spark still adds a WHERE 1=0 suffix.
    assert("SELECT * FROM table" + autofix + " WHERE 1=0" ==
      dialect.getSchemaQuery("table" + autofix))
    // Handle query without WHERE clause:
    assert("SeLeCt * FROM table2 WHERE 1=0" ==
      dialect.getSchemaQuery("SeLeCt * FROM table2" + autofix))
    // Handle query with WHERE clause and JOIN:
    assert("SeLeCT a.*, b.b FROM a INNER JOIN b ON a.x = b.y  WHERE 1=0" ==
      dialect.getSchemaQuery(
        "SeLeCT a.*, b.b FROM a INNER JOIN b ON a.x = b.y WhErE a.a = 1 AND a.b = 2" + autofix))

  }
}
