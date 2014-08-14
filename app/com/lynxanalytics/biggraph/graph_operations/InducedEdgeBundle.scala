package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object InducedEdgeBundle {
  class Input(induceSrc: Boolean, induceDst: Boolean) extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val srcSubset = if (induceSrc) vertexSet else null
    val dstSubset = if (induceDst) vertexSet else null
    // There is no fundamental reason this couldn't work with injections. We can implement that
    // if/when we need it.
    val srcInjection = if (induceSrc) edgeBundle(srcSubset, src, EdgeBundleProperties.embedding) else null
    val dstInjection = if (induceDst) edgeBundle(dstSubset, dst, EdgeBundleProperties.embedding) else null
    val edges = edgeBundle(src, dst)
  }
  class Output(induceSrc: Boolean, induceDst: Boolean)(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val induced = {
      val src = if (induceSrc) inputs.srcSubset else inputs.src
      val dst = if (induceDst) inputs.dstSubset else inputs.dst
      edgeBundle(src.entity, dst.entity)
    }
    val embedding = edgeBundle(induced.asVertexSet, inputs.edges.asVertexSet)
  }
}
import InducedEdgeBundle._
case class InducedEdgeBundle(induceSrc: Boolean = true, induceDst: Boolean = true)
    extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(induceSrc, induceDst)

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output(induceSrc, induceDst)(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.edges.rdd
    val srcInduced = if (!induceSrc) edges else {
      val srcSubset = inputs.srcSubset.rdd
      val bySrc = edges
        .map { case (id, edge) => (edge.src, (id, edge)) }
        .toSortedRDD(srcSubset.partitioner.get)
        .sortedJoin(srcSubset)
        .mapValues { case (idEdge, _) => idEdge }
      bySrc.values
    }
    val dstInduced = if (!induceDst) srcInduced else {
      val dstSubset = inputs.dstSubset.rdd
      val byDst = srcInduced
        .map { case (id, edge) => (edge.dst, (id, edge)) }
        .toSortedRDD(dstSubset.partitioner.get)
        .sortedJoin(dstSubset)
        .mapValues { case (idEdge, _) => idEdge }
      byDst.values
    }
    val induced = dstInduced.toSortedRDD(edges.partitioner.get)
    output(o.induced, induced)
    output(o.embedding, induced.mapValuesWithKeys { case (id, _) => Edge(id, id) })
  }

  override val isHeavy = true
}
