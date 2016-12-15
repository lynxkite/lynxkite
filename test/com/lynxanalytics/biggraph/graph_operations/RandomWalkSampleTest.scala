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
    5 -> Seq(1),
    6 -> Seq(1),
    7 -> Seq(1),
    8 -> Seq(1),
    9 -> Seq(1),
    10 -> Seq(1)
  )).result

  test("too large sample size") {
    val op = RandomWalkSample(0.1, g.vs.rdd.count() + 10, 0)
    val output = op(op.vs, g.vs)(op.es, g.es).result
    val nodesNotInSample = output.verticesInSample.rdd.filter(_._2 == 0.0)
    val edgesNotInSample = output.edgesInSample.rdd.filter(_._2 == 0.0)
    assert(nodesNotInSample.count() == 0)
    assert(edgesNotInSample.count() == 0)
  }

  test("one node sample") {
    val op = RandomWalkSample(0.1, 1, 0)
    val output = op(op.vs, g.vs)(op.es, g.es).result
    assert(output.verticesInSample.rdd.filter(_._2 > 0.0).count() == 1)
    assert(output.edgesInSample.rdd.filter(_._2 > 0.0).count() == 0)
  }
}
