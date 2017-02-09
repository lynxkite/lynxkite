package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import org.scalatest.FunSuite

class RandomWalkSampleTest extends FunSuite with TestGraphOp {
  val g = SmallTestGraph(Map(
    0 -> Seq(1),
    1 -> Seq(2),
    2 -> Seq(3),
    3 -> Seq(4),
    4 -> Seq(5, 6, 7, 8, 9, 10),
    5 -> Seq(0),
    6 -> Seq(0),
    7 -> Seq(0),
    8 -> Seq(0),
    9 -> Seq(0),
    10 -> Seq(0)
  )).result

  test("one long walk") {
    val op = RandomWalkSample(1, 1, 0.01, 0)
    val output = op(op.vs, g.vs)(op.es, g.es).result
    val vs = output.vertexFirstVisited.rdd.collect()
    val es = output.edgeFirstTraversed.rdd.collect()
    assert(vs.count(_._2 < 3.0) == 3)
    assert(vs.count(_._2 < Double.MaxValue) == vs.length)
    assert(es.count(_._2 < 3.0) == 2)
    assert(es.count(_._2 < Double.MaxValue) == es.length)
  }

  test("two short walks") {
    val op = RandomWalkSample(2, 1, 0.999, 0)
    val output = op(op.vs, g.vs)(op.es, g.es).result
    assert(output.vertexFirstVisited.rdd.filter(_._2 < Double.MaxValue).count() == 2)
    assert(output.edgeFirstTraversed.rdd.filter(_._2 < Double.MaxValue).count() == 0)
  }

  test("unconnected graph") {
    val unconnectedG = SmallTestGraph(Map(
      0 -> Seq(1),
      1 -> Seq(0),
      2 -> Seq(3),
      3 -> Seq(2)
    )).result
    val op = RandomWalkSample(1, 100, 0.5, 0)
    val output = op(op.vs, unconnectedG.vs)(op.es, unconnectedG.es).result
    assert(output.vertexFirstVisited.rdd.filter(_._2 < Double.MaxValue).count() == 2)
    assert(output.edgeFirstTraversed.rdd.filter(_._2 < Double.MaxValue).count() == 2)
  }
}
