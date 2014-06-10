package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd
import org.scalatest.FunSuite
import scala.util.Random

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

case class RandomSets(size: Int, vsize: Int, seed: Int) extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]): Boolean = sources.isEmpty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val rand = new Random(seed)
    val sig = vertexAttributes(target.sources)
    val maker = sig.maker
    val idx = sig.writeIndex[Array[VertexId]]("set")
    val elementIds = (0 until vsize).map(_.toLong)
    val vertices = (0 until size).map { id =>
      val set =
        rand.shuffle(elementIds.toSeq).take(rand.nextInt(elementIds.size + 1)).sorted.toArray
      (id.toLong, maker.make.set(idx, set))
    }
    val edges = new rdd.EmptyRDD[Edge[DenseAttributes]](sc)
    return new SimpleGraphData(target, sc.parallelize(vertices), edges)
  }

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[Array[Long]]("set").signature

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = AttributeSignature.empty
}

class SetOverlapForCCTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("Check for a few random sets") {
    val graphManager = cleanGraphManager("checkrandomsofcc")
    val dataManager = cleanDataManager("checkrandomsofcc")
    val rnd = new Random(0)
    for (i <- (0 until 20)) {
      val graph = graphManager.deriveGraph(
        Seq(), RandomSets(rnd.nextInt(100), rnd.nextInt(30) + 1, rnd.nextInt()))
      val overlap = rnd.nextInt(6) + 1
      val normalSOCC = graphManager.deriveGraph(
        Seq(graphManager.deriveGraph(Seq(graph), SetOverlap("set", overlap))),
        ConnectedComponents("cc"))
      val forCCSOCC = graphManager.deriveGraph(
        Seq(graphManager.deriveGraph(Seq(graph), UniformOverlapForCC("set", overlap))),
        ConnectedComponents("cc"))

      def getCCMap(g: BigGraph): Map[Int, Int] = {
        val idx = g.vertexAttributes.readIndex[VertexId]("cc")
        dataManager.obtainData(g).vertices
          .map { case (id, attr) => (id.toInt, attr(idx).toInt) }
          .collect
          .toMap
      }
      ConnectedComponentsTest.assertSameComponents(getCCMap(normalSOCC), getCCMap(forCCSOCC))
    }
  }
}
