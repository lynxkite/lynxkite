// Calculates embeddedness, the number of mutual friends between two people, as an edge attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HLLUtils
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus

object ApproxEmbeddedness extends OpFromJson {
  private val bitsParameter = NewParameter("bits", 12)
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val embeddedness = edgeAttribute[Double](inputs.es.entity)
  }
  def fromJson(j: JsValue) = ApproxEmbeddedness(bitsParameter.fromJson(j))
}
import ApproxEmbeddedness._
case class ApproxEmbeddedness(bits: Int) extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = bitsParameter.toJson(bits)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val edges = inputs.es.rdd
    val nonLoopEdges = edges.filter { case (_, e) => e.src != e.dst }
    val edgePartitioner = edges.partitioner.get
    val hll = HLLUtils(bits)

    val outNeighborHLLs = nonLoopEdges
      .map { case (_, e) => e.src -> hll.hllFromObject(e.dst) }
      .reduceByKey(hll.union)
      .sortUnique(edgePartitioner)
    val inNeighborHLLs = nonLoopEdges
      .map { case (_, e) => e.dst -> hll.hllFromObject(e.src) }
      .reduceByKey(hll.union)
      .sortUnique(edgePartitioner)
    val allNeighborHLLs = outNeighborHLLs.fullOuterJoin(inNeighborHLLs)
      .mapValues { case (out, in) => hll.union(out, in) }

    val bySrc = nonLoopEdges.map { case (eid, e) => e.src -> (e.dst, eid) }
    val bySrcHLLs = HybridRDD(bySrc, edgePartitioner, even = true)
      .lookupAndRepartition(allNeighborHLLs)
    val byDst = bySrcHLLs.map { case (src, ((dst, eid), srcHLL)) => dst -> (src, eid, srcHLL) }
    val byDstHLLs = HybridRDD(byDst, edgePartitioner, even = true)
      .lookup(allNeighborHLLs)
    val embeddedness = byDstHLLs.map {
      case (dst, ((src, eid, srcHLL), dstHLL)) => eid -> hll.intersectSize(srcHLL, dstHLL).toDouble
    }.sortUnique(edgePartitioner)

    output(o.embeddedness, embeddedness)
  }
}
