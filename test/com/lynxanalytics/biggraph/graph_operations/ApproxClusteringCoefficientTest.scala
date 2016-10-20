package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ApproxClusteringCoefficientTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = ApproxClusteringCoefficient(bits = 8)
    val out = op(op.vs, g.vertices)(op.es, g.edges).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.5, 1 -> 0.5, 2 -> 1.0, 3 -> 1.0))
  }

  test("directed triangle") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0)))().result
    val op = ApproxClusteringCoefficient(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.5, 1 -> 0.5, 2 -> 0.5))
  }

  test("bi-directed triangle") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1)))().result
    val op = ApproxClusteringCoefficient(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 1.0, 1 -> 1.0, 2 -> 1.0))
  }

  test("line graph") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq()))().result
    val op = ApproxClusteringCoefficient(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0))
  }

  test("loop edge") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(1, 2)))().result
    val op = ApproxClusteringCoefficient(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.0, 1 -> 0.0))
  }

  test("parallel edges") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0, 0, 0)))().result
    val op = ApproxClusteringCoefficient(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.5, 1 -> 0.5, 2 -> 0.5))
  }

  test("big neighborhood") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2, 3), 1 -> Seq(2), 2 -> Seq(3), 3 -> Seq(1)))().result
    val op = ApproxClusteringCoefficient(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.5, 1 -> 0.5, 2 -> 0.5, 3 -> 0.5))
  }
}
