package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

class PageRankTest
    extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  // Creates the graph specified by `nodes` and applies PageRank to it.
  // Returns the resulting attribute in an easy-to-use format.
  def getPageRank(nodes: Seq[(Int, Seq[Int])]): Map[Int, Int] = {
    val graphManager = cleanGraphManager("SetOverlapTest")
    val dataManager = cleanDataManager("SetOverlapTest")
    val bareGraph = graphManager.deriveGraph(Seq(), GraphByEdgeLists(nodes))
    val inputGraph = graphManager.deriveGraph(
      Seq(bareGraph), ConstantDoubleEdgeAttribute("weight", 1.0))
    val outputGraph = graphManager.deriveGraph(
      Seq(inputGraph), PageRank("weight", "pr", 0.5, 3))
    val idx = outputGraph.vertexAttributes.readIndex[Double]("pr")
    val vertices = dataManager.obtainData(outputGraph).vertices
    return vertices.map({ case (n, da) => (n.toInt, math.round(10 * da(idx)).toInt) }).collect.toMap
  }

  test("three islands") {
    val nodes = Seq(0 -> Seq(), 1 -> Seq(), 2 -> Seq())
    val expectation = Map(0 -> 10, 1 -> 10, 2 -> 10)
    assert(getPageRank(nodes) == expectation)
  }

  test("triangle") {
    val nodes = Seq(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))
    val expectation = Map(0 -> 10, 1 -> 10, 2 -> 10)
    assert(getPageRank(nodes) == expectation)
  }

  test("island and line") {
    val nodes = Seq(0 -> Seq(), 1 -> Seq(2), 2 -> Seq(1))
    val expectation = Map(0 -> 6, 1 -> 12, 2 -> 12)
    assert(getPageRank(nodes) == expectation)
  }

  test("long line") {
    val nodes = Seq(0 -> Seq(1), 1 -> Seq(0, 2), 2 -> Seq(1, 3), 3 -> Seq(2))
    val expectation = Map(0 -> 8, 1 -> 12, 2 -> 12, 3 -> 8)
    assert(getPageRank(nodes) == expectation)
  }
}
