package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

case class UpperBoundFilter(bound: Double) extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexAttribute[Double]('attr, 'vs, create = true)
    .outputVertexSet('fvs)
    .outputEdgeBundle('projection, 'vs -> 'fvs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val attr = inputs.vertexAttributes('attr).runtimeSafeCast[Double].rdd
    val fattr = attr.filter { case (id, v) => v <= bound }
    outputs.putVertexSet('fvs, fattr.mapValues(_ => ()))
    val projection = fattr.map({ case (id, v) => id -> Edge(id, id) }).partitionBy(attr.partitioner.get)
    outputs.putEdgeBundle('projection, projection)
  }
}
