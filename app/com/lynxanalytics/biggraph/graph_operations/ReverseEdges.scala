package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

case class ReverseEdges() extends MetaGraphOperation {
  def signature = newSignature
    .inputEdgeBundle('esAB, 'vsA -> 'vsB, create = true)
    .outputEdgeBundle('esBA, 'vsB -> 'vsA)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val edgesBA = inputs.edgeBundles('esAB).rdd.mapValues(e => Edge(e.dst, e.src))
    outputs.putEdgeBundle('esBA, edgesBA)
  }
}
