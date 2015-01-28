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
    val res = op(op.edges, g.edges)(op.eattr, g.weight).result.dstAttr
    assert(res.rdd.collect.toMap == Map(0 -> 5.0, 1 -> 5.0))
  }
  test("standard deviation - constant") {
    val example = ExampleGraph()().result
    val attr = {
      val op = AddConstantDoubleAttribute(100.0)
      op(op.vs, example.vertices).result.attr
    }
    val devConst = {
      val op = AggregateAttributeToScalar(Aggregator.StdDev())
      op(op.attr, attr).result.aggregated.value
    }
    assert(devConst == 0.0)
  }
  test("standard deviation - combine partitions") {
    val g = SmallTestGraph(
      Map(1 -> Seq(1), 2 -> Seq(1, 2), 3 -> Seq(1, 2, 3), 4 -> Seq(1, 2, 3, 4)),
      numPartitions = 2).result
    val attr = {
      val op = OutDegree()
      op(op.es, g.es).result.outDegree
    }
    val devWeight = {
      val op = AggregateAttributeToScalar(Aggregator.StdDev())
      op(op.attr, attr).result.aggregated.value
    }
    assert(devWeight == {
      val n = 4
      val mean = (1.0 + 2.0 + 3.0 + 4.0) / n
      val devs = List(Math.abs(mean - 1), Math.abs(mean - 2), Math.abs(mean - 3), Math.abs(mean - 4))
      val variance = devs.map(Math.pow(_, 2)).reduce(_ + _) / (n - 1) // n-1 is for sample variance
      Math.sqrt(variance)
    })
    assert(devWeight == 1.2909944487358056)
  }
}
