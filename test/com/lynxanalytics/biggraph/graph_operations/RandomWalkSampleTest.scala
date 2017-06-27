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
  val numOfNodes = 11
  val numOfEdges = 16

  test("one long walk") {
    val (vs, es) = run(RandomWalkSample(1, 1, 0.01, 0), g)
    assert(vs.count(_._2 < 3.0) == 3)
    assert(visited(vs) == numOfNodes)
    assert(es.count(_._2 < 3.0) == 2)
    assert(visited(es) == numOfEdges)
  }

  test("two short walks") {
    val (vs, es) = run(RandomWalkSample(2, 1, 0.999, 0), g)
    assert(visited(vs) == 2)
    assert(visited(es) == 0)
  }

  test("unconnected graph") {
    val unconnectedG = SmallTestGraph(Map(
      0 -> Seq(1),
      1 -> Seq(0),
      2 -> Seq(3),
      3 -> Seq(2)
    )).result
    val (vs, es) = run(RandomWalkSample(1, 100, 0.5, 0), unconnectedG)
    val visitedNodes = vs.map(_._1)
    val traversedEdges = es.map(_._1)
    if (visitedNodes.contains(0)) {
      assert(visitedNodes.sameElements(Array(0, 1)))
      assert(traversedEdges.sameElements(Array(0, 1)))
    } else {
      assert(visitedNodes.sameElements(Array(2, 3)))
      assert(traversedEdges.sameElements(Array(2, 3)))
    }
  }

  private def run(op: RandomWalkSample, g: SmallTestGraph.Output) = {
    val output = op(op.vs, g.vs)(op.es, g.es).result
    val vs = output.vertexFirstVisited.rdd.collect()
    val es = output.edgeFirstTraversed.rdd.collect()
    (vs, es)
  }

  private def visited(attribute: Array[(ID, Double)]) = attribute.length
}
