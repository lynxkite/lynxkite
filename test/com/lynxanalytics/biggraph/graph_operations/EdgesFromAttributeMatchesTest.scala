package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class EdgesFromAttributeMatchesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = EdgesFromAttributeMatches[String]()
    val res = op(op.attr, g.gender).result
    assert(res.edges.toPairSeq == Seq(0 -> 2, 0 -> 3, 2 -> 0, 2 -> 3, 3 -> 0, 3 -> 2))
  }
}

class EdgesFromBipartiteAttributeMatchesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g1 = ExampleGraph()().result
    val g2 = SmallTestGraph(Map(0 -> Seq(1)))().result
    val g2g = AddVertexAttribute.run(g2.vs, Map(0 -> "Male", 1 -> "Female", 2 -> "Female"))
    val op = EdgesFromBipartiteAttributeMatches[String]()
    val res = op(op.fromAttr, g1.gender)(op.toAttr, g2g).result
    assert(res.edges.toPairSeq == Seq(0 -> 0, 1 -> 1, 1 -> 2, 2 -> 0, 3 -> 0))
  }
}

class EdgesFromUniqueBipartiteAttributeMatchesTest extends FunSuite with TestGraphOp {
  test("import attributes for existing vertex set from table") {
    val table = SmallTestGraph(
      Map(1 -> Seq(), 2 -> Seq(), 3 -> Seq(), 4 -> Seq()))().result
    val idColumn = AddVertexAttribute.run(table.vs,
      Map(1 -> "Eve", 2 -> "Adam", 5 -> "Isolated Joe", 6 -> "John"))

    val graph = {
      // 0: Adam
      // 1: Eve
      // 2: Bob
      // 3: Isolated Joe
      val op = ExampleGraph()
      op.result
    }

    val op = EdgesFromUniqueBipartiteAttributeMatches()
    val result = op(op.toAttr, idColumn)(
      op.fromAttr, graph.name).result
    assert(Seq(Edge(0, 2), Edge(1, 1), Edge(3, 5)) ==
      result.edges.rdd.values.collect.toSeq)
  }
}

class EdgesFromLookupAttributeMatchesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g1 = ExampleGraph()().result
    val g2 = SmallTestGraph(Map(0 -> Seq(1)))().result
    val g2g = AddVertexAttribute.run(g2.vs, Map(0 -> "Male", 1 -> "Female"))
    val op = EdgesFromLookupAttributeMatches()
    val res = op(op.fromAttr, g1.gender)(op.toAttr, g2g).result
    assert(res.edges.toPairSeq == Seq(0 -> 0, 1 -> 1, 2 -> 0, 3 -> 0))
  }
}
