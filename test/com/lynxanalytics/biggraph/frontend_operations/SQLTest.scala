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
      .box("SQL1", Map("sql" -> "select * from vertices order by id"))
      .table
    assert(table.schema.map(_.name) == Seq("age", "gender", "id", "income", "location", "name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq(20.3, "Male", 0, 1000.0, Seq(40.71448, -74.00598), "Adam"),
      Seq(18.2, "Female", 1, null, Seq(47.5269674, 19.0323968), "Eve"),
      Seq(50.3, "Male", 2, 2000.0, Seq(1.352083, 103.819836), "Bob"),
      Seq(2.0, "Male", 3, null, Seq(-33.8674869, 151.2069902), "Isolated Joe")))
  }

  test("edges table") {
    val table = box("Create example graph")
      .box("SQL1", Map("sql" -> "select * from edges order by edge_comment"))
      .table
    assert(table.schema.map(_.name) == Seq("dst_age", "dst_gender", "dst_id", "dst_income",
      "dst_location", "dst_name", "edge_comment", "edge_weight", "src_age", "src_gender", "src_id",
      "src_income", "src_location", "src_name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq(18.2, "Female", 1, null, Seq(47.5269674, 19.0323968), "Eve", "Adam loves Eve", 1.0,
        20.3, "Male", 0, 1000.0, Seq(40.71448, -74.00598), "Adam"),
      Seq(20.3, "Male", 0, 1000.0, Seq(40.71448, -74.00598), "Adam", "Bob envies Adam", 3.0, 50.3,
        "Male", 2, 2000.0, Seq(1.352083, 103.819836), "Bob"),
      Seq(18.2, "Female", 1, null, Seq(47.5269674, 19.0323968), "Eve", "Bob loves Eve", 4.0, 50.3,
        "Male", 2, 2000.0, Seq(1.352083, 103.819836), "Bob"),
      Seq(20.3, "Male", 0, 1000.0, Seq(40.71448, -74.00598), "Adam", "Eve loves Adam", 2.0, 18.2,
        "Female", 1, null, Seq(47.5269674, 19.0323968), "Eve")))
  }

  test("edge_attributes table") {
    val table = box("Create example graph")
      .box("SQL1", Map("sql" -> "select * from edge_attributes order by comment"))
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
      .box("SQL1", Map("sql" -> """
        select base_name, segment_id, segment_size
        from `connected_components|belongs_to` order by base_id"""))
      .table
    assert(table.schema.map(_.name) == Seq("base_name", "segment_id", "segment_size"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam", 0, 3.0), Seq("Eve", 0, 3.0), Seq("Bob", 0, 3.0), Seq("Isolated Joe", 3, 1.0)))
  }

  test("sql on vertices") {
    val table = box("Create example graph")
      .box("SQL1", Map("sql" -> "select name from vertices where age < 40"))
      .table
    assert(table.schema.map(_.name) == Seq("name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("sql with empty results") {
    val table = box("Create example graph")
      .box("SQL1", Map("sql" -> "select id from vertices where id = 11"))
      .table
    assert(table.schema.map(_.name) == Seq("id"))
    val data = table.df.collect.toSeq
    assert(data == List())
  }

  test("sql file reading is disabled") {
    val file = getClass.getResource("/controllers/noread.csv").toString
    val ws = box("Create example graph")
      .box("SQL1", Map("sql" -> s"select * from csv.`$file`"))
    val e = intercept[AssertionError] {
      ws.table
    }
    assert(e.getMessage.contains("No such table: csv."))
  }

  test("three inputs") {
    val one = box("Create example graph")
    val two = box("Create example graph")
    val three = box("Create example graph")
    val table = box("SQL3", Map("sql" -> """
      select one.edge_comment, two.name, three.name
      from `one|edges` as one
      join `two|vertices` as two
      join `three|vertices` as three
      where one.src_name = two.name and one.dst_name = three.name
      """), Seq(one, two, three)).table
    assert(table.schema.map(_.name) == Seq("edge_comment", "name", "name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Bob envies Adam", "Bob", "Adam"),
      Seq("Eve loves Adam", "Eve", "Adam"),
      Seq("Adam loves Eve", "Adam", "Eve"),
      Seq("Bob loves Eve", "Bob", "Eve")))
  }
}
