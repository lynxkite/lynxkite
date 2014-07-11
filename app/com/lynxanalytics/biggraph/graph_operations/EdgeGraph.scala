package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class EdgeGraph() extends MetaGraphOperation {
  def signature = newSignature
    .inputGraph('vs, 'es)
    .outputGraph('newVS, 'newES)
    .outputEdgeBundle('link, 'vs -> 'newVS)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val sc = rc.sparkContext
    val edgePartitioner = rc.defaultPartitioner
    val edges = inputs.edgeBundles('es).rdd
    val newVS = edges.mapValues(_ => ())

    val edgesBySource = edges.map {
      case (id, edge) => (edge.src, id)
    }.groupByKey(edgePartitioner)
    val edgesByDest = edges.map {
      case (id, edge) => (edge.dst, id)
    }.groupByKey(edgePartitioner)

    val newES = edgesBySource.join(edgesByDest).flatMap {
      case (vid, (outgoings, incomings)) =>
        for {
          outgoing <- outgoings
          incoming <- incomings
        } yield Edge(incoming, outgoing)
    }
    outputs.putVertexSet('newVS, newVS)
    outputs.putEdgeBundle('newES, newES.fastNumbered(edgePartitioner))
    // Just to connect to the results.
    outputs.putEdgeBundle('link, sc.emptyRDD[(ID, Edge)].partitionBy(edgePartitioner))
  }
}
