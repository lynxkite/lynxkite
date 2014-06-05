package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx

import com.lynxanalytics.biggraph.graph_api

import graph_api._
import graph_api.attributes._

case class UpperBoundFilter(attributeName: String, bound: Double) extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) =
    (sources.size == 1) && sources.head.vertexAttributes.canRead[Double](attributeName)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sourceGraph = target.sources.head
    val sourceData = manager.obtainData(sourceGraph)
    val idx = sourceGraph.vertexAttributes.readIndex[Double](attributeName)
    val vertices = sourceData.vertices.filter { case (id, attr) => attr(idx) <= bound }
    val vertexPartitioner =
      vertices.partitioner.getOrElse(manager.runtimeContext.defaultPartitioner)
    val vertexIds = vertices.mapValues { _ => () }.partitionBy(vertexPartitioner)
    val edges = sourceData.edges
      .map(e => (e.srcId, e)).partitionBy(vertexPartitioner)
      .join(vertexIds)
      .map { case (_, (e, _)) => (e.dstId, e) }.partitionBy(vertexPartitioner)
      .join(vertexIds)
      .map { case (_, (e, _)) => e }
    return new SimpleGraphData(target, vertices, edges)
  }

  def vertexAttributes(sources: Seq[BigGraph]) = sources.head.vertexAttributes

  def edgeAttributes(sources: Seq[BigGraph]) = sources.head.edgeAttributes

  override def targetProperties(sources: Seq[BigGraph]): BigGraphProperties =
    sources.head.properties
}
