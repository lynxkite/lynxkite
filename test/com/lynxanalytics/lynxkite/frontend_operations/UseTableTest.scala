package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_util
import com.lynxanalytics.lynxkite.controllers._

class UseTableTest extends OperationsTestBase {
  test("number literals become numbers") {
    val t = box("Create example graph").box("SQL1", Map("sql" -> "select 1.0 as x, 'a', 'b'"))
    val g = t.box("Use table as graph", Map("src" -> "a", "dst" -> "b")).project
    assert(get(g.edgeAttributes("x").runtimeSafeCast[Double]).values.toSeq == Seq(1.0))
  }

  test("SQL floats become numbers") {
    val t = box("Create example graph")
      .box("SQL1", Map("sql" -> "select cast(1.0 as float) as x, 'a', 'b'"))
    val g = t.box("Use table as graph", Map("src" -> "a", "dst" -> "b")).project
    assert(get(g.edgeAttributes("x").runtimeSafeCast[Double]).values.toSeq == Seq(1.0))
  }

  test("conflict resolution in 'Use table as vertex attributes'") {
    val eg = box("Create example graph")
    def sql(q: String) = eg.box("SQL1", Map("sql" -> q))
    val t = sql("select 1.0 as age, 'Eve' as n")
    def use(t: TestBox, params: Map[String, String], attr: String = "age") = {
      val g = box(
        "Use table as vertex attributes",
        Map("id_attr" -> "name", "id_column" -> "n") ++ params,
        Seq(eg, t))
        .project
      get(g.vertexAttributes(attr).runtimeSafeCast[Double])
    }
    assert(use(t, Map()) == Map(0 -> 20.3, 1 -> 1.0, 2 -> 50.3, 3 -> 2.0))
    assert(use(t, Map("if_exists" -> "Merge, prefer the table's version")) ==
      Map(0 -> 20.3, 1 -> 1.0, 2 -> 50.3, 3 -> 2.0))
    assert(
      use(
        sql("select 1.0 as income, 'Eve' as n union all select 2.0, 'Bob'"),
        Map("if_exists" -> "Merge, prefer the graph's version"),
        "income") ==
        Map(0 -> 1000.0, 1 -> 1.0, 2 -> 2000.0))
    assert(use(t, Map("if_exists" -> "Keep the graph's version")) ==
      Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    assert(use(t, Map("if_exists" -> "Use the table's version")) == Map(1 -> 1.0))
    assert(
      use(
        sql("select 18.2 as age, 'Eve' as n"),
        Map("if_exists" -> "Merge, report error on conflict")) ==
        Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    val mismatch = intercept[Exception] {
      use(t, Map("if_exists" -> "Merge, report error on conflict"))
    }
    lazy val exceptions: Stream[Throwable] = mismatch #:: exceptions.map(_.getCause)
    assert(exceptions.exists(_.getMessage.contains(
      "assertion failed: age does not match on Eve: 1.0 <> 18.2")))
    val conflict = intercept[AssertionError] {
      use(t, Map("if_exists" -> "Disallow this"))
    }
    assert(conflict.getMessage.contains("Cannot import column `age`. Attribute already exists."))
  }

  test("conflict resolution in 'Use table as edge attributes'") {
    val eg = box("Create example graph")
    val t = eg.box("SQL1", Map("sql" -> "select 99.0 as weight, 'Eve loves Adam' as n"))
    def use(t: TestBox, params: Map[String, String]) = {
      val g = box(
        "Use table as edge attributes",
        Map("id_attr" -> "comment", "id_column" -> "n") ++ params,
        Seq(eg, t))
        .project
      get(g.edgeAttributes("weight").runtimeSafeCast[Double])
    }
    assert(use(t, Map()) == Map(0 -> 1.0, 1 -> 99.0, 2 -> 3.0, 3 -> 4.0))
    assert(use(t, Map("if_exists" -> "Merge, prefer the table's version")) ==
      Map(0 -> 1.0, 1 -> 99.0, 2 -> 3.0, 3 -> 4.0))
    assert(use(t, Map("if_exists" -> "Merge, prefer the graph's version")) ==
      Map(0 -> 1.0, 1 -> 2.0, 2 -> 3.0, 3 -> 4.0))
    assert(use(t, Map("if_exists" -> "Keep the graph's version")) ==
      Map(0 -> 1.0, 1 -> 2.0, 2 -> 3.0, 3 -> 4.0))
    assert(use(t, Map("if_exists" -> "Use the table's version")) == Map(1 -> 99.0))
    assert(
      use(
        eg.box("SQL1", Map("sql" -> "select 2.0 as weight, 'Eve loves Adam' as n")),
        Map("if_exists" -> "Merge, report error on conflict")) ==
        Map(0 -> 1.0, 1 -> 2.0, 2 -> 3.0, 3 -> 4.0))
    val mismatch = intercept[Exception] {
      use(t, Map("if_exists" -> "Merge, report error on conflict"))
    }
    lazy val exceptions: Stream[Throwable] = mismatch #:: exceptions.map(_.getCause)
    assert(exceptions.exists(_.getMessage.contains(
      "assertion failed: weight does not match on Eve loves Adam: 99.0 <> 2.0")))
    val conflict = intercept[AssertionError] {
      use(t, Map("if_exists" -> "Disallow this"))
    }
    assert(conflict.getMessage.contains("Cannot import column `weight`. Attribute already exists."))
  }
}
