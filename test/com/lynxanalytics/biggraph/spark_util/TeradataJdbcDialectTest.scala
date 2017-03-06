package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite

class TeradataJdbcDialectTest extends FunSuite {

  test("test TeradataDialect") {
    val magicMarker = "/*LYNX-TD-SCHEMA-AUTO-FIX*/"

    val dialect = new TeradataDialect()

    // This is testing Spark's behavior. Needed for reference for the below tests.
    {
      val query = "table"
      val expected = "SELECT * FROM table WHERE 1=0"
      assert(expected == dialect.getSchemaQuery(query))
    }

    // For a table, autofix should just remove the magic marker
    // and fall back to Spark's logic.
    {
      val query = "table" + magicMarker
      val expected = "SELECT * FROM table WHERE 1=0"
      assert(expected == dialect.getSchemaQuery(query))
    }

    // Handle query without WHERE clause:
    {
      val query = "(SeLeCt * FROM table2) tname1" + magicMarker
      val expected = "SeLeCt * FROM table2 WHERE 1=0"
      assert(expected == dialect.getSchemaQuery(query))
    }

    // Handle query with WHERE clause and JOIN:
    {
      val query = "(SeLeCT a.*, b.b FROM a INNER JOIN b ON a.x = b.y WhErE a.a = 1 AND a.b = 2) tname2" + magicMarker
      val expected = "SeLeCT a.*, b.b FROM a INNER JOIN b ON a.x = b.y  WHERE 1=0"
      assert(expected == dialect.getSchemaQuery(query))
    }

    // Complex query with WHERE clause at start of a line:
    {
      val query = """(SELECT a.*, b.ACCNT_NUM
FROM INVOC_HIST a
INNER JOIN INVOC b
ON a.Invoc_id = b.Invoc_Id
WHERE CAST ('2016-02-12' AS TMESTAMP(0) FORMAT 'YYYY-MM-DD') between a.Start_Date and a.End_Date) source_table
""" + magicMarker
      val expected = """SELECT a.*, b.ACCNT_NUM
FROM INVOC_HIST a
INNER JOIN INVOC b
ON a.Invoc_id = b.Invoc_Id
 WHERE 1=0"""
      assert(expected == dialect.getSchemaQuery(query))
    }

    // Query with WHERE clause at the end of a line
    {
      val query = """(SELECT * from xyz WhErE
x =1
) source_table
""" + magicMarker
      val expected = "SELECT * from xyz  WHERE 1=0"
      assert(expected == dialect.getSchemaQuery(query))
    }

    intercept[AssertionError] {
      dialect.getSchemaQuery("(select where where) tname3" + magicMarker)
    }

    intercept[AssertionError] {
      dialect.getSchemaQuery("(select where where ) tname3" + magicMarker)
    }

    intercept[AssertionError] {
      dialect.getSchemaQuery("(select where where)) tname3" + magicMarker)
    }
  }
}
