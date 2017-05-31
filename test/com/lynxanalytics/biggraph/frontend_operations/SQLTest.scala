package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import org.apache.spark

class SQLTest extends OperationsTestBase {
  private def toSeq(row: spark.sql.Row): Seq[Any] = {
    row.toSeq.map {
      case r: spark.sql.Row => toSeq(r)
      case x => x
    }
  }

  test("SQL basics") {
    val table = box("Create example graph")
      .box("SQL", Map("sql" -> "select * from `stuff|vertices` order by id"))
      .table
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam", Seq(40.71448, -74.00598), 20.3, 0, 1000.0, "Male"),
      Seq("Eve", Seq(47.5269674, 19.0323968), 18.2, 1, null, "Female"),
      Seq("Bob", Seq(1.352083, 103.819836), 50.3, 2, 2000.0, "Male"),
      Seq("Isolated Joe", Seq(-33.8674869, 151.2069902), 2.0, 3, null, "Male")))
  }
}
