package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateTest extends FunSuite with TestGraphOp {
  test("example graph components") {
    val example = ExampleGraph()().result
    val components = {
      val op = ConnectedComponents()
      op(op.es, example.edges).result
    }
    def run[Attr, Res](
      agg: LocalAggregator[Attr, Res], attr: Attribute[Attr]) = {
      val op = AggregateByEdgeBundle(agg)
      op(op.bySrc, HybridEdgeBundle.bySrc(components.belongsTo))(op.attr, attr)
        .result.attr.rdd.collect.toMap
    }
    assert(run(Aggregator.AsSet[String](), example.name) ==
      Map(0 -> Set("Adam", "Eve"), 2 -> Set("Bob"), 3 -> Set("Isolated Joe")))
    assert(run(Aggregator.Count[Double](), example.age) ==
      Map(0 -> 2, 2 -> 1, 3 -> 1))
    assert(run(Aggregator.Sum(), example.age) ==
      Map(0 -> 38.5, 2 -> 50.3, 3 -> 2.0))
    assert(run(Aggregator.Average(), example.age) ==
      Map(0 -> 19.25, 2 -> 50.3, 3 -> 2.0))
    assert(run(Aggregator.Median(), example.age) ==
      Map(0 -> 19.25, 2 -> 50.3, 3 -> 2.0))
    assert(run(Aggregator.MostCommon[String](), example.gender) ==
      Map(0 -> "Female", 2 -> "Male", 3 -> "Male"))
    assert(run(Aggregator.CountMostCommon[String](), example.gender) ==
      Map(0 -> 1.0, 2 -> 1.0, 3 -> 1.0))
    // Cannot predict output except for isolated points.
    val firsts = run(Aggregator.First[String](), example.name)
    assert(firsts.contains(0))
    assert(firsts - 0 == Map(2 -> "Bob", 3 -> "Isolated Joe"))
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
