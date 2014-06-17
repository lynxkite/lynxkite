package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx

import com.lynxanalytics.biggraph.graph_api

import graph_api._
import graph_api.attributes._

case class RemoveNonSymmetricEdges() extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 1)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val source = target.sources.head
    val sourceData = manager.obtainData(source)
    val bySource = sourceData.edges.map(e => (e.srcId, e)).groupByKey()
    val byDest = sourceData.edges.map(e => (e.dstId, e.srcId)).groupByKey().mapValues(_.toSet)
    val edges = bySource.join(byDest).flatMap {
      case (vertexId, (outEdges, inEdgeSources)) =>
        outEdges.filter(outEdge => inEdgeSources.contains(outEdge.dstId))
    }
    return new SimpleGraphData(target, sc.union(sourceData.vertices), edges)
  }

  def vertexAttributes(sources: Seq[BigGraph]) = sources.head.vertexAttributes

  def edgeAttributes(sources: Seq[BigGraph]) = sources.head.edgeAttributes

  override def targetProperties(sources: Seq[BigGraph]) =
    new BigGraphProperties(symmetricEdges = true)
}
