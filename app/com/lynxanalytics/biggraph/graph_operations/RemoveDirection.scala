package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.graphx

import com.lynxanalytics.biggraph.graph_api

import graph_api._
import graph_api.attributes._

case class RemoveDirection() extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 1)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val source = target.sources.head
    val sourceData = manager.obtainData(source)
    val edges = sourceData.edges
      .flatMap(e => Iterator(e, new graphx.Edge(e.dstId, e.srcId, e.attr)))
    return new SimpleGraphData(target, sc.union(sourceData.vertices), edges)
  }

  def vertexAttributes(sources: Seq[BigGraph]) = sources.head.edgeAttributes

  def edgeAttributes(sources: Seq[BigGraph]) = sources.head.vertexAttributes

  override def targetProperties(sources: Seq[BigGraph]) =
    sources.head.properties.copy(symmetricEdges = true)
}
