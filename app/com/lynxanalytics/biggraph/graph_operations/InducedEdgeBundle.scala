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
      .partitionBy(srcSubset.partitioner.get).toSortedRDD
      .sortedJoin(srcSubset)
      .sortedMapValues { case (idEdge, _) => idEdge }
    val byDst = bySrc
      .map { case (vid, (id, edge)) => (edge.dst, (id, edge)) }
      .partitionBy(dstSubset.partitioner.get).toSortedRDD
      .sortedJoin(dstSubset)
      .sortedMapValues { case (idEdge, _) => idEdge }
    output(o.induced, byDst.values.partitionBy(edges.partitioner.get))
  }

  override val isHeavy = true
}
