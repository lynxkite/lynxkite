package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateTest extends FunSuite with TestGraphOp {
  test("example graph components") {
    val example = ExampleGraph()().result
    val components = {
      val op = ConnectedComponents()
      op(op.es, example.edges).result
    }
    val count = {
      val op = AggregateByEdgeBundle(Aggregator.Count[Double]())
      op(op.connection, components.belongsTo)(op.attr, example.age).result
    }
    assert(count.attr.rdd.collect.toSet == Set(0 -> 2, 2 -> 1, 3 -> 1))
    val sum = {
      val op = AggregateByEdgeBundle(Aggregator.Sum())
      op(op.connection, components.belongsTo)(op.attr, example.age).result
    }
    assert(sum.attr.rdd.collect.toSet == Set(0 -> 38.5, 2 -> 50.3, 3 -> 2.0))
    val average = {
      val op = AggregateByEdgeBundle(Aggregator.Average())
      op(op.connection, components.belongsTo)(op.attr, example.age).result
    }
    assert(average.attr.rdd.collect.toSet == Set(0 -> 19.25, 2 -> 50.3, 3 -> 2.0))
    val first = {
      val op = AggregateByEdgeBundle(Aggregator.First[String]())
      op(op.connection, components.belongsTo)(op.attr, example.name).result
    }
    // Cannot predict output except for isolated points.
    val firsts = first.attr.rdd.collect.toSet
    assert(firsts.size == 3)
    assert(firsts.contains(3L -> "Isolated Joe"))
  }
  test("example graph attribute aggregates") {
    val example = ExampleGraph()().result
    val sumAge = {
      val op = AggregateAttributeToScalar(Aggregator.Sum())
      op(op.attr, example.age).result.aggregated.value
    }
    assert(sumAge == 90.8)
  }
  // TODO: this is not defined on isolated vertices
  test("example graph - weighted out degree") {
    val g = ExampleGraph()().result
    val op = AggregateFromEdges[Double, Double](Aggregator.Sum())
    val res = op(op.edges, g.edges)(op.eattr, g.weight.asVertexAttribute).result.srcAttr
    assert(res.rdd.collect.toMap == Map(0 -> 1.0, 1 -> 2.0, 2 -> 7.0))
  }
}
