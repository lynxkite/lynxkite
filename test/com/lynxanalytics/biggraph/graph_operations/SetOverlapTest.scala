package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.graphx.VertexId
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

// Quick way to make a graph where the nodes have a set-valued attribute.
case class GraphBySetAttribute(attr: String, nodes: Seq[(Int, Seq[Int])]) extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 0)
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[Array[Long]](attr).signature
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val sig = vertexAttributes(Seq())
    val maker = sig.maker
    val idx = sig.writeIndex[Array[Long]](attr)
    val vertices = nodes.map({
      case (vid, set) => {
        val da = maker.make()
        da.set(idx, set.map(_.toLong).toArray)
        (vid.toLong, da)
      }
    })
    return new SimpleGraphData(
      target,
      sc.parallelize(vertices),
      new spark.rdd.EmptyRDD[spark.graphx.Edge[DenseAttributes]](sc)
    )
  }
}

class SetOverlapTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  // Creates the graph specified by `nodes` and applies SetOverlap to it.
  // Returns the resulting edges in an easy-to-use format.
  def getOverlaps(nodes: Seq[(Int, Seq[Int])], minOverlap: Int): Seq[(Int, Int, Int)] = {
    val graphManager = cleanGraphManager("SetOverlapTest")
    val dataManager = cleanDataManager("SetOverlapTest")
    val inputGraph = graphManager.deriveGraph(Seq(), GraphBySetAttribute("set", nodes))
    val outputGraph = graphManager.deriveGraph(Seq(inputGraph), SetOverlap("set", minOverlap))
    val idx = outputGraph.edgeAttributes.readIndex[Int]("set_overlap")
    val edges = dataManager.obtainData(outputGraph).edges
    return edges.map(e => (e.srcId.toInt, e.dstId.toInt, e.attr(idx))).collect.sorted.toSeq
  }

  test("triangle") {
    val overlaps = getOverlaps(Seq(
      0 -> Seq(1, 2),
      1 -> Seq(2, 3),
      2 -> Seq(1, 3)),
      minOverlap = 1)
    assert(overlaps == Seq((0, 1, 1), (0, 2, 1), (1, 0, 1), (1, 2, 1), (2, 0, 1), (2, 1, 1)))
  }

  test("minOverlap too high") {
    val overlaps = getOverlaps(Seq(
      0 -> Seq(1, 2),
      1 -> Seq(2, 3),
      2 -> Seq(1, 3)),
      minOverlap = 2)
    assert(overlaps == Seq())
  }

  test("> 70 nodes") {
    // Tries to trigger the use of longer prefixes.
    val N = 100
    assert(SetOverlap.SetListBruteForceLimit < N)
    val overlaps = getOverlaps(
      (0 to N).map(i => i -> Seq(-3, -2, -1, i)),
      minOverlap = 2)
    val expected = for {
      a <- (0 to N)
      b <- (0 to N)
      if a != b
    } yield (a, b, 3)
    assert(overlaps == expected)
  }
}
