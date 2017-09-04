// Calculates approximate embeddedness using HLLs, the number of mutual friends between
// two people, as an edge attribute. The attribute is only defined on non-loop edges.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HLLUtils
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark

object ApproxEmbeddedness extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val embeddedness = edgeAttribute[Double](inputs.es.entity)
  }
  def fromJson(j: JsValue) = ApproxEmbeddedness((j \ "bits").as[Int])
}

private[graph_operations] case class NeighborHLLs(
    nonLoopEdges: EdgeBundleRDD,
    partitioner: spark.Partitioner,
    hll: HLLUtils) {
  val outNeighborHLLs = nonLoopEdges
    .map { case (_, e) => e.src -> hll.hllFromObject(e.dst) }
    .reduceByKey(hll.union)
    .sortUnique(partitioner)
  val inNeighborHLLs = nonLoopEdges
    .map { case (_, e) => e.dst -> hll.hllFromObject(e.src) }
    .reduceByKey(hll.union)
    .sortUnique(partitioner)
  // For every non isolated vertex a HLL of all its incoming and outgoing neighbors.
  val allNeighborHLLs = outNeighborHLLs.fullOuterJoin(inNeighborHLLs)
    .mapValues { case (out, in) => hll.union(out, in) }
}

import ApproxEmbeddedness._
case class ApproxEmbeddedness(bits: Int) extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("bits" -> bits)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val edges = inputs.es.rdd
    val nonLoopEdges = edges.filter { case (_, e) => e.src != e.dst }
    val partitioner = edges.partitioner.get
    val hll = HLLUtils(bits)

    val allNeighborHLLs = NeighborHLLs(nonLoopEdges, partitioner, hll).allNeighborHLLs
    allNeighborHLLs.persist(spark.storage.StorageLevel.DISK_ONLY)

    // Join the HLL of neighbors on both the dsts and srcs of the non loop edges.
    val bySrc = nonLoopEdges.map { case (eid, e) => e.src -> (e.dst, eid) }
    val bySrcHLLs = HybridRDD.of(bySrc, partitioner, even = true)
      .lookupAndRepartition(allNeighborHLLs)
    val byDst = bySrcHLLs.map { case (src, ((dst, eid), srcHLL)) => dst -> (src, eid, srcHLL) }
    val byDstHLLs = HybridRDD.of(byDst, partitioner, even = true)
      .lookup(allNeighborHLLs)
    // Embeddedness is the size of the intersect of the src and dst HLLs.
    val embeddedness = byDstHLLs.map {
      case (dst, ((src, eid, srcHLL), dstHLL)) => eid -> hll.intersectSize(srcHLL, dstHLL).toDouble
    }.sortUnique(edges.partitioner.get)

    output(o.embeddedness, embeddedness)
  }
}
