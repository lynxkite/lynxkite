package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object InducedEdgeBundle {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val srcSubset = vertexSet
    val dstSubset = vertexSet
    // There is no fundamental reason this couldn't work with injections. We can implement that
    // if/when we need it.
    val srcInjection = edgeBundle(srcSubset, src, EdgeBundleProperties.embedding)
    val dstInjection = edgeBundle(dstSubset, dst, EdgeBundleProperties.embedding)
    val edges = edgeBundle(src, dst)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val induced = edgeBundle(inputs.srcSubset.entity, inputs.dstSubset.entity)
  }
}
import InducedEdgeBundle._
case class InducedEdgeBundle() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.edges.rdd
    val srcSubset = inputs.srcSubset.rdd
    val dstSubset = inputs.dstSubset.rdd
    val bySrc = edges
      .map { case (id, edge) => (edge.src, (id, edge)) }
      .toSortedRDD(srcSubset.partitioner.get)
      .sortedJoin(srcSubset)
      .mapValues { case (idEdge, _) => idEdge }
    val byDst = bySrc
      .map { case (vid, (id, edge)) => (edge.dst, (id, edge)) }
      .toSortedRDD(dstSubset.partitioner.get)
      .sortedJoin(dstSubset)
      .mapValues { case (idEdge, _) => idEdge }
    output(o.induced, byDst.values.toSortedRDD(edges.partitioner.get))
  }

  override val isHeavy = true
}
