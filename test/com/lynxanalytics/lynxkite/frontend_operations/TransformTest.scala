package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import org.apache.spark
import org.apache.spark.sql.Row

class TransformTest extends OperationsTestBase {
  test("transform table") {
    val table = box("Create example graph")
      .box("SQL1", Map("sql" -> "select * from vertices"))
      .box("Transform", Map("new_age" -> "age * 2"))
      .table
    assert(table.schema.map(_.name) == Seq("age", "gender", "id", "income", "location", "name"))
    val data = table.df.collect.toSeq.map(row => SQLTest.toSeq(row))
    assert(data == Seq(
      Seq(40.6, "Male", "0", 1000.0, Seq(40.71448, -74.00598), "Adam"),
      Seq(36.4, "Female", "1", null, Seq(47.5269674, 19.0323968), "Eve"),
      Seq(100.6, "Male", "2", 2000.0, Seq(1.352083, 103.819836), "Bob"),
      Seq(4.0, "Male", "3", null, Seq(-33.8674869, 151.2069902), "Isolated Joe"),
    ))
  }
}
