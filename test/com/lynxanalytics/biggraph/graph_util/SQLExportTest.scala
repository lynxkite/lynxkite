package com.lynxanalytics.biggraph.graph_util

import anorm.SQL
import java.sql
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.ExampleGraph

class SQLExportTest extends FunSuite with TestGraphOp {
  def linesOf(s: String): Seq[String] = {
    s.trim.split("\n", -1).map(_.trim)
  }

  test("check dump") {
    val g = ExampleGraph()().result
    val export = SQLExport(
      "example_graph",
      g.vertices,
      g.vertexAttributes.mapValues(_.entity).map(identity))
    assert(linesOf(export.creation) == linesOf("""
      DROP TABLE IF EXISTS example_graph;
      CREATE TABLE example_graph (age DOUBLE PRECISION, income DOUBLE PRECISION, gender TEXT, name TEXT);
      """))
    assert(export.inserts.collect.toSeq.map(linesOf(_)).flatten == linesOf("""
      INSERT INTO example_graph VALUES
        (20.3, 1000.0, "Male", "Adam");
      INSERT INTO example_graph VALUES
        (18.2, NULL, "Female", "Eve");
      INSERT INTO example_graph VALUES
        (50.3, 2000.0, "Male", "Bob");
      INSERT INTO example_graph VALUES
        (2.0, NULL, "Male", "Isolated Joe");
      """))
  }

  test("export to SQLite") {
    val g = ExampleGraph()().result
    val export = SQLExport(
      "example_graph",
      g.vertices,
      g.vertexAttributes.mapValues(_.entity).map(identity))
    val db = s"sqlite:${dataManager.repositoryPath}/test-db"
    export.insertInto(db)
    implicit val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val q = SQL("SELECT name FROM example_graph WHERE age < 20")
    assert(q().map(row => row[String]("name")).sorted == Seq("Eve", "Isolated Joe"))
  }
}
