package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class ConcatenateBundles() extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vsA)
    .inputVertexSet('vsB)
    .inputVertexSet('vsC)
    .inputEdgeBundle('edgesAB, 'vsA -> 'vsB)
    .inputEdgeAttribute[Double]('weightsAB, 'edgesAB)
    .inputEdgeBundle('edgesBC, 'vsB -> 'vsC)
    .inputEdgeAttribute[Double]('weightsBC, 'edgesBC)
    .outputEdgeBundle('edgesAC, 'vsA -> 'vsC)
    .outputEdgeAttribute[Double]('weightsAC, 'edgesAC)
  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val edgesAB = inputs.edgeBundles('edgesAB).rdd
    val edgesBC = inputs.edgeBundles('edgesBC).rdd
    val weightsAB = inputs.edgeAttributes('weightsAB).runtimeSafeCast[Double].rdd
    val weightsBC = inputs.edgeAttributes('weightsBC).runtimeSafeCast[Double].rdd
    val weightedEdgesAB = edgesAB.join(weightsAB)
    val weightedEdgesBC = edgesBC.join(weightsBC)

    val partitioner = inputs.vertexSets('vsB).rdd.partitioner.getOrElse(rc.defaultPartitioner)
    val BA = weightedEdgesAB.map { case (_, (edge, weight)) => edge.dst -> (edge.src, weight) }.partitionBy(partitioner)
    val BC = weightedEdgesBC.map { case (_, (edge, weight)) => edge.src -> (edge.dst, weight) }.partitionBy(partitioner)

    val AC = BA.join(BC).map {
      case (_, ((vertexA, weightAB), (vertexC, weightBC))) => (Edge(vertexA, vertexC), weightAB * weightBC)
    }.reduceByKey(_ + _) // TODO: possibility to define arbitrary concat functions as JS

    val numberedAC = AC.fastNumbered(rc.defaultPartitioner)

    outputs.putEdgeBundle(
      'edgesAC, numberedAC.mapValues { case (edge, weight) => edge })
    outputs.putEdgeAttribute(
      'weightsAC, numberedAC.mapValues { case (edge, weight) => weight })
  }
}
