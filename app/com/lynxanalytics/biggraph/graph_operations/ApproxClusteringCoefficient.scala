// Calculates for each vertex how close its neighborhood is to a clique approximately.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HLLUtils
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

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
    val simpleGraphEdges =
      nonLoopEdges.flatMap { case (_, e) => Iterator(e.src -> e.dst, e.dst -> e.src) }.distinct

    val bySrcHLLs = HybridRDD.of(simpleGraphEdges, partitioner, even = true)
      .lookupAndRepartition(n.allNeighborHLLs)
    val byDst = bySrcHLLs.map { case (src, (dst, srcHLL)) => dst -> (src, srcHLL) }
    val byDstHLLs = HybridRDD.of(byDst, partitioner, even = true)
      .lookup(n.outNeighborHLLs)

    // For every edge sum up the common neighbors of src and out-neighbors dst
    // and the size of the neighborhood.
    val commonNeighbors = byDstHLLs.map {
      case (dst, ((src, srcHLL), dstHLL)) => {
        src -> (hll.intersectSize(srcHLL, dstHLL).toDouble, 1L)
      }
    }
    val clusteringCoeffNonIsolated = commonNeighbors
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues {
        case (numEdges, n) =>
          if (n > 1) {
            numEdges / (n * (n - 1))
          } else {
            1.0 // Vertices with only one neighbor have a cc of 1.0 by definition.
          }
      }
      .sortUnique(vertices.partitioner.get)

    val clusteringCoeff =
      vertices.sortedLeftOuterJoin(clusteringCoeffNonIsolated)
        // Because of approximation > 1.0 values are possible and have to be bounded.
        .mapValues { case (_, cc) => cc.getOrElse(1.0) min 1.0 }
    output(o.clustering, clusteringCoeff)
  }
}
