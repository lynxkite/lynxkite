// Calculates for each vertex how close its neighborhood is to a clique approximetaly.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HLLUtils
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus

import org.apache.spark

object ApproxClusteringCoefficient extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val clustering = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = ApproxClusteringCoefficient((j \ "bits").as[Int])
}
import ApproxClusteringCoefficient._
case class ApproxClusteringCoefficient(bits: Int) extends TypedMetaGraphOp[GraphInput, Output] {
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
    val vertices = inputs.vs.rdd
    val nonLoopEdges = edges.filter { case (_, e) => e.src != e.dst }
    val partitioner = edges.partitioner.get
    val hll = HLLUtils(bits)

    val n = NeighborHLLs(nonLoopEdges, partitioner, hll)
    n.outNeighborHLLs.persist(spark.storage.StorageLevel.DISK_ONLY)
    n.allNeighborHLLs.persist(spark.storage.StorageLevel.DISK_ONLY)

    // Make the graph bi-directed and remove parallel edges.
    val simpleGraphEdges = (
      nonLoopEdges.map { case (_, e) => e.src -> e.dst } ++
      nonLoopEdges.map { case (_, e) => e.dst -> e.src }).distinct

    val bySrcHLLs = HybridRDD(simpleGraphEdges, partitioner, even = true)
      .lookupAndRepartition(n.allNeighborHLLs)
    val byDst = bySrcHLLs.map { case (src, (dst, srcHLL)) => dst -> (src, srcHLL) }
    val byDstHLLs = HybridRDD(byDst, partitioner, even = true)
      .lookup(n.outNeighborHLLs)

    // For every edge sum up the common neighbors of src and dst and divide the sum
    // with the total neighbors of src.
    val commonNeighbors = byDstHLLs.map {
      case (dst, ((src, srcHLL), dstHLL)) => {
        val n = srcHLL.cardinality
        if (n > 1) {
          src -> hll.intersectSize(srcHLL, dstHLL).toDouble / (n * (n - 1))
        } else {
          src -> 0.0
        }
      }
    }
    val clusteringCoeffNonIsolated = commonNeighbors
      .reduceByKey(_ + _)
      .sortUnique(vertices.partitioner.get)

    val clusteringCoeff =
      vertices.sortedLeftOuterJoin(clusteringCoeffNonIsolated)
        .mapValues { case (_, cc) => cc.getOrElse(1.0) max 1.0 }
    output(o.clustering, clusteringCoeff)
  }
}
