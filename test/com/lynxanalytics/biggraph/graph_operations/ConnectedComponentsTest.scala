package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

// Quick way to make a graph with no attributes from edge lists.
case class GraphByEdgeLists(nodes: Seq[(Int, Seq[Int])]) extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 0)
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val sig = vertexAttributes(Seq())
    val maker = sig.maker
    val vertices = nodes.map({ case (n, edges) => (n.toLong, maker.make()) })
    val edges = nodes.flatMap({
      case (n, edges) =>
        edges.map(e => Edge(n.toLong, e.toLong, maker.make()))
    })
    return new SimpleGraphData(
      target,
      sc.parallelize(vertices, 1),
      sc.parallelize(edges, 1)
    )
  }

  override def targetProperties(inputGraphSpecs: Seq[BigGraph]) =
    new BigGraphProperties(symmetricEdges = true)
}

object ConnectedComponentsTest {
  def assertSameComponents(comp1: Map[Int, Int], comp2: Map[Int, Int]): Unit = {
    val mapping = scala.collection.mutable.Map[Int, Int]()
    assert(comp1.size == comp2.size, "Unexpected size")
    for (k <- comp1.keys) {
      assert(comp2.contains(k), s"Missing key: $k")
      val c1 = comp1(k)
      val c2 = comp2(k)
      if (mapping.contains(c1)) {
        assert(mapping(c1) == c2, s"Unable to match components $c1 and $c2")
      } else {
        mapping(c1) = c2
      }
    }
  }
}
class ConnectedComponentsTest
    extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  import ConnectedComponentsTest._

  // Creates the graph specified by `nodes` and applies ConnectedComponents to it.
  // Returns the resulting component attributes in an easy-to-use format.
  def getComponents(nodes: Seq[(Int, Seq[Int])], local: Boolean): Map[Int, Int] = {
    val graphManager = cleanGraphManager("SetOverlapTest")
    val dataManager = cleanDataManager("SetOverlapTest")
    val inputGraph = graphManager.deriveGraph(Seq(), GraphByEdgeLists(nodes))
    ConnectedComponents.maxEdgesProcessedLocally = if (local) 100000 else 0
    val outputGraph = graphManager.deriveGraph(
      Seq(inputGraph), ConnectedComponents("component"))
    val idx = outputGraph.vertexAttributes.readIndex[Long]("component")
    val vertices = dataManager.obtainData(outputGraph).vertices
    return vertices.map({ case (n, da) => (n.toInt, da(idx).toInt) }).collect.toMap
  }

  test("three islands") {
    val nodes = Seq(0 -> Seq(), 1 -> Seq(), 2 -> Seq())
    val expectation = Map(0 -> 0, 1 -> 1, 2 -> 2)
    assertSameComponents(getComponents(nodes, local = true), expectation)
    assertSameComponents(getComponents(nodes, local = false), expectation)
  }

  test("triangle") {
    val nodes = Seq(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))
    val expectation = Map(0 -> 0, 1 -> 0, 2 -> 0)
    assertSameComponents(getComponents(nodes, local = true), expectation)
    assertSameComponents(getComponents(nodes, local = false), expectation)
  }

  test("island and line") {
    val nodes = Seq(0 -> Seq(), 1 -> Seq(2), 2 -> Seq(1))
    val expectation = Map(0 -> 0, 1 -> 1, 2 -> 1)
    assertSameComponents(getComponents(nodes, local = true), expectation)
    assertSameComponents(getComponents(nodes, local = false), expectation)
  }

  test("long line") {
    val nodes = Seq(0 -> Seq(1), 1 -> Seq(0, 2), 2 -> Seq(1, 3), 3 -> Seq(2))
    val expectation = Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0)
    assertSameComponents(getComponents(nodes, local = true), expectation)
    assertSameComponents(getComponents(nodes, local = false), expectation)
  }
}
