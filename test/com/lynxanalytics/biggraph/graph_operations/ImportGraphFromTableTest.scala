package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ImportGraphFromTableTest extends FunSuite with TestGraphOp {
  test("import edges for existing vertex set from table") {
    val table = SmallTestGraph(
      Map(1 -> Seq(), 2 -> Seq(), 3 -> Seq(), 4 -> Seq()))().result
    val srcColumn = {
      val op = AddVertexAttribute(
        Map(1 -> "Adam", 2 -> "Eve", 3 -> "Isolated Joe", 4 -> "Bob"))
      op(op.vs, table.vs).result.attr
    }
    val dstColumn = {
      val op = AddVertexAttribute(
        Map(1 -> "Eve", 2 -> "Adam", 3 -> "Hank", 4 -> "Eve"))
      op(op.vs, table.vs).result.attr
    }
    val graph = {
      val op = ExampleGraph()
      op.result
    }

    val op = ImportEdgeListForExistingVertexSetFromTable()
    val result = op(op.srcVidAttr, graph.name)(
      op.dstVidAttr, graph.name)(
        op.srcVidColumn, srcColumn)(
          op.dstVidColumn, dstColumn).result
    assert(Seq((1, Edge(0, 1)), (2, Edge(1, 0)), (4, Edge(2, 1))) ==
      result.edges.rdd.collect.toSeq)
  }

  test("import attributes for existing vertex set from table") {
    val table = SmallTestGraph(
      Map(1 -> Seq(), 2 -> Seq(), 3 -> Seq(), 4 -> Seq()))().result
    val idColumn = {
      val op = AddVertexAttribute(
        Map(1 -> "Eve", 2 -> "Adam", 5 -> "Isolated Joe", 6 -> "John"))
      op(op.vs, table.vs).result.attr
    }

    val graph = {
      // 0: Adam
      // 1: Eve
      // 2: Bob
      // 3: Isolated Joe
      val op = ExampleGraph()
      op.result
    }

    val op = ImportAttributesForExistingVertexSetFromTable()
    val result = op(op.idColumn, idColumn)(
      op.idAttr, graph.name).result
    assert(Seq(Edge(0, 2), Edge(1, 1), Edge(3, 5)) ==
      result.pullFunction.rdd.values.collect.toSeq)
  }
}
