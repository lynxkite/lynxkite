package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

case class InducedEdgeBundle() extends MetaGraphOperation {
  def signature = newSignature
    .inputEdgeBundle('input, 'src -> 'dst, create = true)
    .inputVertexSet('srcSubset) // The user needs to make sure it's a subset of 'src
    .inputVertexSet('dstSubset) // The user needs to make sure it's a subset of 'dst
    .outputEdgeBundle('induced, 'srcSubset -> 'dstSubset)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val input = inputs.edgeBundles('input).rdd
    val srcSubset = inputs.vertexSets('srcSubset).rdd
    val dstSubset = inputs.vertexSets('dstSubset).rdd
    val bySrc = input
      .map { case (id, edge) => (edge.src, (id, edge)) }
      .partitionBy(srcSubset.partitioner.get)
      .join(srcSubset)
      .mapValues { case (idEdge, _) => idEdge }
    val byDst = bySrc
      .map { case (vid, (id, edge)) => (edge.dst, (id, edge)) }
      .partitionBy(dstSubset.partitioner.get)
      .join(dstSubset)
      .mapValues { case (idEdge, _) => idEdge }
    outputs.putEdgeBundle(
      'induced,
      byDst.values.partitionBy(input.partitioner.get))
  }

  override val isHeavy = true
}
