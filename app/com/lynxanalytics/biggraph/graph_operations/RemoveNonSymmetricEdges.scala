package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

case class RemoveNonSymmetricEdges() extends MetaGraphOperation {
  def signature = newSignature
    .inputGraph('vs, 'es)
    .outputEdgeBundle('symmetric, 'vs -> 'vs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val vsPart = inputs.vertexSets('vs).rdd.partitioner.get
    val es = inputs.edgeBundles('es).rdd
    val bySource = es.map {
      case (id, e) => e.src -> (id, e)
    }.groupByKey(vsPart)
    val byDest = es.map {
      case (id, e) => e.dst -> e.src
    }.groupByKey(vsPart).mapValues(_.toSet)
    val edges = bySource.join(byDest).flatMap {
      case (vertexId, (outEdges, inEdgeSources)) =>
        outEdges.collect {
          case (id, outEdge) if inEdgeSources.contains(outEdge.dst) => id -> outEdge
        }
    }
    outputs.putEdgeBundle('symmetric, edges.partitionBy(es.partitioner.get))
  }

  override val isHeavy = true
}
