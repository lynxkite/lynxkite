package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ModularityTest extends AnyFunSuite with TestGraphOp {
  test("example graph uncut") {
    val eg = ExampleGraph()().result
    val clusters = CreateVertexSet(1)().result.vs
    val belongsTo = {
      val op = AddEdgeBundle(Seq((0, 0), (1, 0), (2, 0), (3, 0)))
      op(op.vsA, eg.vertices)(op.vsB, clusters).result.esAB
    }
    val modularity = {
      val op = Modularity()
      op(op.edges, eg.edges)(op.weights, eg.weight)(op.belongsTo, belongsTo).result.modularity
    }
    assert(modularity.value == 0.0)
  }
  test("example graph cut stupidly") {
    val eg = ExampleGraph()().result
    val clusters = CreateVertexSet(2)().result.vs
    val belongsTo = {
      val op = AddEdgeBundle(Seq((0, 1), (1, 0), (2, 0), (3, 0)))
      op(op.vsA, eg.vertices)(op.vsB, clusters).result.esAB
    }
    val modularity = {
      val op = Modularity()
      op(op.edges, eg.edges)(op.weights, eg.weight)(op.belongsTo, belongsTo).result.modularity
    }
    assert(math.abs(modularity.value - (-5 * 1 / 100.0 + 4 / 10.0 - 9 * 5 / 100.0)) < 0.0000001)
  }
}
