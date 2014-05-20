package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx

import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.spark_util

import graph_api._
import graph_api.attributes._
import spark_util.RDDUtils

case class EdgeGraph() extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 1)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val rc = manager.runtimeContext
    val sc = rc.sparkContext
    val edgePartitioner = rc.defaultPartitioner
    val sourceData = manager.obtainData(target.sources.head)
    val edgesWithIds = RDDUtils.fastNumbered(sourceData.edges)
    val newVertices = edgesWithIds.map{case (id, edge) => (id, edge.attr)}

    val edgesBySource = edgesWithIds.map{
        case (id, edge) => (edge.srcId, id)
      }.groupByKey(edgePartitioner)
    val edgesByDest = edgesWithIds.map{
        case (id, edge) => (edge.dstId, id)
      }.groupByKey(edgePartitioner)

    val newEdges = edgesBySource.join(edgesByDest).join(sourceData.vertices).flatMap{
        case (vid, ((outgoings, incommings), vattr)) =>
          for (outgoing <- outgoings;
               incomming <- incommings) yield new graphx.Edge(incomming, outgoing, vattr)
      }

    return new SimpleGraphData(target, newVertices, newEdges)
  }

  def vertexAttributes(sources: Seq[BigGraph]) = sources.head.edgeAttributes

  def edgeAttributes(sources: Seq[BigGraph]) = sources.head.vertexAttributes
}
