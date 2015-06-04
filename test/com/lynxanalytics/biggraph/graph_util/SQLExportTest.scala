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

  test("export to SQLite") {
    val g = ExampleGraph()().result
    val export = SQLExport(
      "example graph",
      g.vertices,
      Map[String, Attribute[_]](
        "age" -> g.age, "gender" -> g.gender,
        "income" -> g.income, "name" -> g.name))

    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    export.insertInto(db, delete = true)
    implicit val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val q1 = SQL("SELECT name FROM \"example graph\" WHERE age < 20")
    assert(q1().map(row => row[String]("name")).sorted == Seq("Eve", "Isolated Joe"))

    export.insertInto(db, delete = false)
    val q2 = SQL("SELECT name FROM \"example graph\" WHERE age < 20")
    assert(q2().map(row => row[String]("name")).sorted == Seq("Eve", "Eve", "Isolated Joe", "Isolated Joe"))
  }
}
