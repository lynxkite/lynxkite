package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx

import com.lynxanalytics.biggraph.graph_api

import graph_api._
import graph_api.attributes._

case class DropAttributes() extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 1)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sourceData = manager.obtainData(target.sources.head)
    val maker = AttributeSignature.empty.maker

    val vertices = sourceData.vertices.mapValues(attr => maker.make)
    val edges = sourceData.edges.map(e => new graphx.Edge(e.srcId, e.dstId, maker.make))

    return new SimpleGraphData(target, vertices, edges)
  }

  def vertexAttributes(sources: Seq[BigGraph]) = AttributeSignature.empty

  def edgeAttributes(sources: Seq[BigGraph]) = AttributeSignature.empty

  override def targetProperties(sources: Seq[BigGraph]): BigGraphProperties =
    sources.head.properties
}
