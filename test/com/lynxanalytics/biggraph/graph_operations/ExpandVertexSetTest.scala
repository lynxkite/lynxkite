package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.graphx.VertexId
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

case class ExpandVertexSetTestGraph(
    setAttr: String,
    longAttr: String,
    nodes: Seq[(Int, Seq[Int], Int)]) extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 0)
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty
      .addAttribute[Array[Long]](setAttr)
      .addAttribute[Long](longAttr).signature
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val sig = vertexAttributes(Seq())
    val maker = sig.maker
    val setIdx = sig.writeIndex[Array[Long]](setAttr)
    val longIdx = sig.writeIndex[Long](longAttr)
    val vertices = nodes.map {
      case (vid, set, l) =>
        val da = maker.make()
        da.set(setIdx, set.map(_.toLong).toArray)
        da.set(longIdx, l.toLong)
        (vid.toLong, da)
    }
    return new SimpleGraphData(
      target,
      sc.parallelize(vertices),
      sc.emptyRDD[spark.graphx.Edge[DenseAttributes]]
    )
  }
}

class ExpandVertexSetTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  def getExpanded(nodes: Seq[(Int, Seq[Int], Int)]): Seq[(Int, Seq[Int], Seq[Int])] = {
    val graphManager = cleanGraphManager("ExpandVertexSetTest")
    val dataManager = cleanDataManager("ExpandVertexSetTest")
    val inputGraph = graphManager.deriveGraph(
      Seq(), ExpandVertexSetTestGraph("recipients", "num", nodes))
    val outputGraph = graphManager.deriveGraph(
      Seq(inputGraph), ExpandVertexSet("recipients", "senders"))
    val sendersIdx = outputGraph.vertexAttributes.readIndex[Array[Long]]("senders")
    val numIdx = outputGraph.vertexAttributes.readIndex[Array[Long]]("num")
    val vertices = dataManager.obtainData(outputGraph).vertices
    return vertices.map {
      case (id, da) => (id.toInt, da(sendersIdx).toSeq.map(_.toInt), da(numIdx).toSeq.map(_.toInt))
    }.collect.sortBy(_._1).toSeq
  }

  test("three to three") {
    val expanded = getExpanded(Seq(
      (0, Seq(11, 22), 100),
      (1, Seq(11, 33), 200),
      (2, Seq(22, 33), 300)))
    val expected = Seq(
      (11, Seq(0, 1), Seq(100, 200)),
      (22, Seq(0, 2), Seq(100, 300)),
      (33, Seq(1, 2), Seq(200, 300)))
    assert(expanded == expected)
  }
}
