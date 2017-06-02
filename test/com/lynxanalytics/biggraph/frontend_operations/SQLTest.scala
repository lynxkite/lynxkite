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

  test("vertices table") {
    val table = box("Create example graph")
      .box("SQL", Map("sql" -> "select * from `stuff|vertices` order by id"))
      .table
    assert(table.schema.map(_.name) == Seq("name", "location", "age", "id", "income", "gender"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam", Seq(40.71448, -74.00598), 20.3, 0, 1000.0, "Male"),
      Seq("Eve", Seq(47.5269674, 19.0323968), 18.2, 1, null, "Female"),
      Seq("Bob", Seq(1.352083, 103.819836), 50.3, 2, 2000.0, "Male"),
      Seq("Isolated Joe", Seq(-33.8674869, 151.2069902), 2.0, 3, null, "Male")))
  }

  test("edges table") {
    val table = box("Create example graph")
      .box("SQL", Map("sql" -> "select * from `stuff|edges` order by edge_comment"))
      .table
    assert(table.schema.map(_.name) == Seq("edge_comment", "edge_weight", "src_name",
      "src_location", "src_age", "src_id", "src_income", "src_gender", "dst_name", "dst_location",
      "dst_age", "dst_id", "dst_income", "dst_gender"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam loves Eve", 1.0, "Adam", Seq(40.71448, -74.00598), 20.3, 0, 1000.0, "Male", "Eve",
        Seq(47.5269674, 19.0323968), 18.2, 1, null, "Female"),
      Seq("Bob envies Adam", 3.0, "Bob", Seq(1.352083, 103.819836), 50.3, 2, 2000.0, "Male", "Adam",
        Seq(40.71448, -74.00598), 20.3, 0, 1000.0, "Male"),
      Seq("Bob loves Eve", 4.0, "Bob", Seq(1.352083, 103.819836), 50.3, 2, 2000.0, "Male", "Eve",
        Seq(47.5269674, 19.0323968), 18.2, 1, null, "Female"),
      Seq("Eve loves Adam", 2.0, "Eve", Seq(47.5269674, 19.0323968), 18.2, 1, null, "Female",
        "Adam", Seq(40.71448, -74.00598), 20.3, 0, 1000.0, "Male")))
  }

  test("edge_attributes table") {
    val table = box("Create example graph")
      .box("SQL", Map("sql" -> "select * from `stuff|edge_attributes` order by comment"))
      .table
    assert(table.schema.map(_.name) == Seq("comment", "weight"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam loves Eve", 1.0),
      Seq("Bob envies Adam", 3.0),
      Seq("Bob loves Eve", 4.0),
      Seq("Eve loves Adam", 2.0)))
  }

  test("belongs_to table") {
    val table = box("Create example graph")
      .box("Find connected components")
      .box("SQL", Map("sql" -> """
        select base_name, segment_id, segment_size
        from `stuff|connected_components|belongs_to` order by base_id"""))
      .table
    assert(table.schema.map(_.name) == Seq("base_name", "segment_id", "segment_size"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam", 0, 3.0), Seq("Eve", 0, 3.0), Seq("Bob", 0, 3.0), Seq("Isolated Joe", 3, 1.0)))
  }

  test("sql on vertices") {
    val table = box("Create example graph")
      .box("SQL", Map("sql" -> "select name from vertices where age < 40"))
      .table
    assert(table.schema.map(_.name) == Seq("name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("sql with empty results") {
    val table = box("Create example graph")
      .box("SQL", Map("sql" -> "select id from vertices where id = 11"))
      .table
    assert(table.schema.map(_.name) == Seq("id"))
    val data = table.df.collect.toSeq
    assert(data == List())
  }

  test("sql file reading is disabled") {
    val file = getClass.getResource("/controllers/noread.csv").toString
    val ws = box("Create example graph")
      .box("SQL", Map("sql" -> s"select * from csv.`$file`"))
    val e = intercept[AssertionError] {
      ws.table
    }
    assert(e.getMessage.contains("No such table: csv."))
  }
}
