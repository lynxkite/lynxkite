package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class EdgesFromSegmentationTest extends AnyFunSuite with TestGraphOp {
  test("on example graph strong components") {
    val g = ExampleGraph()().result
    val components = {
      val op = ConnectedComponents()
      op(op.es, g.edges).result
    }
    val result = {
      val op = EdgesFromSegmentation()
      op(op.belongsTo, components.belongsTo).result
    }
    assert(result.es.toPairSeq.sorted == Seq(0 -> 0, 0 -> 1, 1 -> 0, 1 -> 1, 2 -> 2, 3 -> 3))
  }

  test("on example graph weak components") {
    val g = ExampleGraph()().result
    val es = {
      val op = AddReversedEdges()
      op(op.es, g.edges).result.esPlus
    }
    val components = {
      val op = ConnectedComponents()
      op(op.es, es).result
    }
    val result = {
      val op = EdgesFromSegmentation()
      op(op.belongsTo, components.belongsTo).result
    }
    assert(result.es.toPairSeq.sorted ==
      Seq(0 -> 0, 0 -> 1, 0 -> 2, 1 -> 0, 1 -> 1, 1 -> 2, 2 -> 0, 2 -> 1, 2 -> 2, 3 -> 3))
  }

  // TODO: Add tests with overlapping segments.
}
