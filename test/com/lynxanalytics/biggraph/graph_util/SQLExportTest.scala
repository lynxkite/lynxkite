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
      Map[String, Attribute[_]](
        "age" -> g.age, "gender" -> g.gender,
        "income" -> g.income, "name" -> g.name))
    assert(export.deletion.trim == """
      DROP TABLE IF EXISTS "example_graph";
      """.trim)
    assert(export.creation.trim == """
      CREATE TABLE "example_graph" ("age" DOUBLE PRECISION, "gender" VARCHAR(512), "income" DOUBLE PRECISION, "name" VARCHAR(512));
      """.trim)
    assert(export.inserts.collect.toSeq.map(linesOf(_)).flatten == linesOf("""
      INSERT INTO "example_graph" VALUES (20.3, 'Male', 1000.0, 'Adam');
      INSERT INTO "example_graph" VALUES (18.2, 'Female', NULL, 'Eve');
      INSERT INTO "example_graph" VALUES (50.3, 'Male', 2000.0, 'Bob');
      INSERT INTO "example_graph" VALUES (2.0, 'Male', NULL, 'Isolated Joe');
      """))
  }

  test("export to SQLite") {
    val g = ExampleGraph()().result
    val export = SQLExport(
      "example_graph",
      g.vertices,
      Map[String, Attribute[_]](
        "age" -> g.age, "gender" -> g.gender,
        "income" -> g.income, "name" -> g.name))

    val db = s"sqlite:${dataManager.repositoryPath.sandboxedPath.resolvedName}/test-db"
    export.insertInto(db, delete = true)
    implicit val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val q = SQL("SELECT name FROM example_graph WHERE age < 20")
    assert(q().map(row => row[String]("name")).sorted == Seq("Eve", "Isolated Joe"))
  }
}
