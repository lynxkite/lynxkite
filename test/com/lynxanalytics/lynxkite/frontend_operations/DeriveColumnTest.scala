package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import org.apache.spark
import org.apache.spark.sql.Row

class DeriveColumnTest extends OperationsTestBase {
  test("derive column on table") {
    val table = box("Create example graph")
      .box("SQL1", Map("sql" -> "select * from vertices"))
      .box("Derive column", Map("name" -> "capital name", "value" -> "upper(name)"))
      .table
    assert(table.schema.map(_.name) == Seq("age", "gender", "id", "income", "location", "name", "capital name"))
    val data = table.df.collect.toSeq.map(row => SQLTest.toSeq(row))
    assert(data == Seq(
      Seq(20.3, "Male", "0", 1000.0, Seq(40.71448, -74.00598), "Adam", "ADAM"),
      Seq(18.2, "Female", "1", null, Seq(47.5269674, 19.0323968), "Eve", "EVE"),
      Seq(50.3, "Male", "2", 2000.0, Seq(1.352083, 103.819836), "Bob", "BOB"),
      Seq(2.0, "Male", "3", null, Seq(-33.8674869, 151.2069902), "Isolated Joe", "ISOLATED JOE"),
    ))
  }

  test("derive column on table - overwrite existing column") {
    val table = box("Create example graph")
      .box("SQL1", Map("sql" -> "select * from vertices"))
      .box("Derive column", Map("name" -> "name", "value" -> "upper(name)"))
      .table
    assert(table.schema.map(_.name) == Seq("age", "gender", "id", "income", "location", "name"))
    val data = table.df.collect.toSeq.map(row => SQLTest.toSeq(row))
    assert(data == Seq(
      Seq(20.3, "Male", "0", 1000.0, Seq(40.71448, -74.00598), "ADAM"),
      Seq(18.2, "Female", "1", null, Seq(47.5269674, 19.0323968), "EVE"),
      Seq(50.3, "Male", "2", 2000.0, Seq(1.352083, 103.819836), "BOB"),
      Seq(2.0, "Male", "3", null, Seq(-33.8674869, 151.2069902), "ISOLATED JOE"),
    ))
  }
}
