package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

case class ConcatenateBundles() extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vsA)
    .inputVertexSet('vsB)
    .inputVertexSet('vsC)
    .inputEdgeBundle('edgesAB, 'vsA -> 'vsB)
    .inputEdgeAttribute[Double]('weightsAB, 'edgesAB)
    .inputEdgeBundle('edgesBA, 'vsB -> 'vsC)
    .inputEdgeAttribute[Double]('weightsBC, 'edgesBC)
    .outputEdgeBundle('edgesAC, 'vsA -> 'vsC)
    .outputEdgeAttribute[Double]('weightsAC, 'edgesAC)
  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = ???
}
