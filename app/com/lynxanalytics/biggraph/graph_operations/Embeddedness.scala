// Calculates embeddedness, the number of mutual friends between two people, as an edge attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object Embeddedness extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val embeddedness = edgeAttribute[Double](inputs.es.entity)
  }
  def fromJson(j: JsValue) = Embeddedness()
}
import Embeddedness._
case class Embeddedness() extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val nonLoopEdges = edges.filter { case (_, e) => e.src != e.dst }
    val edgePartitioner = edges.partitioner.get
    val neighbors =
      ClusteringCoefficient.Neighbors(nonLoopEdges, edgePartitioner).allNoIsolated

    val bySrc = edges.map {
      case (eid, e) => e.src -> (eid, e)
    }.sort(edgePartitioner)
    val srcJoined = bySrc.sortedJoin(neighbors)
    val byDst = srcJoined.map {
      case (src, ((eid, e), srcNeighbors)) => e.dst -> (eid, srcNeighbors)
    }.sort(edgePartitioner)
    val dstJoined = byDst.sortedJoin(neighbors)

    val embeddedness = dstJoined.map {
      case (dst, ((eid, srcNeighbors), dstNeighbors)) =>
        eid -> ClusteringCoefficient.sortedIntersectionSize(srcNeighbors, dstNeighbors).toDouble
    }.sortUnique(edgePartitioner)

    output(o.embeddedness, embeddedness)
  }
}
