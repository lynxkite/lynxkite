package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import GraphTestUtils._

class ImportGraphFromTableTest extends FunSuite with TestGraphOp {
  test("import edges for existing vertex set from table") {
    val table = SmallTestGraph(
      Map(1 -> Seq(), 2 -> Seq(), 3 -> Seq(), 4 -> Seq()))().result
    val srcColumn = AddVertexAttribute.run(
      table.vs, Map(1 -> "Adam", 2 -> "Eve", 3 -> "Isolated Joe", 4 -> "Bob"))
    val dstColumn = AddVertexAttribute.run(
      table.vs, Map(1 -> "Eve", 2 -> "Adam", 3 -> "Hank", 4 -> "Eve"))
    val graph = ExampleGraph().result

    val result = ImportEdgesForExistingVertices.run(
      graph.name, graph.name, srcColumn, dstColumn)
    assert(Seq((1, (0, 1)), (2, (1, 0)), (4, (2, 1))) ==
      result.edges.toIdPairSeq)
  }

  test("import edges by ID") {
    val table = SmallTestGraph(
      Map(0 -> Seq(), 1 -> Seq(), 2 -> Seq(), 3 -> Seq()))().result
    val srcColumn = AddVertexAttribute.run(table.vs, Map(0 -> 0L, 1 -> 1L, 2 -> 2L, 3 -> 2L))
    val dstColumn = AddVertexAttribute.run(table.vs, Map(0 -> 1L, 1 -> 0L, 2 -> 0L, 3 -> 1L))
    val graph = ExampleGraph().result

    val result = ImportEdgesForExistingVertices.run(
      graph.vertices.idAttribute, graph.vertices.idAttribute, srcColumn, dstColumn)
    assert(graph.edges.toIdPairSeq == result.edges.toIdPairSeq)
  }
}
