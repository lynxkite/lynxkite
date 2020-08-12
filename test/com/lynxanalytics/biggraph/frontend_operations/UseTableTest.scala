package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.controllers._

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
    val t = eg.box("SQL1", Map("sql" -> "select 1.0 as age, 'Eve' as n"))
    def use(t: TestBox, params: Map[String, String]) = {
      val g = box(
        "Use table as vertex attributes",
        Map("id_attr" -> "name", "id_column" -> "n") ++ params, Seq(eg, t))
        .project
      get(g.vertexAttributes("age").runtimeSafeCast[Double])
    }
    assert(use(t, Map()) == Map(0 -> 20.3, 1 -> 1.0, 2 -> 50.3, 3 -> 2.0))
    assert(use(t, Map("if_exists" -> "Overwrite from the table")) ==
      Map(0 -> 20.3, 1 -> 1.0, 2 -> 50.3, 3 -> 2.0))
    assert(use(t, Map("if_exists" -> "Keep the graph's version")) ==
      Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    assert(use(t, Map("if_exists" -> "Use the table's version")) == Map(1 -> 1.0))
    val mismatch = intercept[Exception] {
      assert(use(t, Map("if_exists" -> "They must match")) ==
        Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    }
    assert(
      use(
        eg.box("SQL1", Map("sql" -> "select 18.2 as age, 'Eve' as n")),
        Map("if_exists" -> "They must match")) ==
        Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    lazy val exceptions: Stream[Throwable] = mismatch #:: exceptions.map(_.getCause)
    assert(exceptions.exists(_.getMessage.contains(
      "assertion failed: age does not match on Eve: Some(1.0) <> Some(18.2)")))
    val conflict = intercept[AssertionError] {
      assert(use(t, Map("if_exists" -> "Disallow this")) ==
        Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    }
    assert(conflict.getMessage.contains("Cannot import column `age`. Attribute already exists."))
  }
}
