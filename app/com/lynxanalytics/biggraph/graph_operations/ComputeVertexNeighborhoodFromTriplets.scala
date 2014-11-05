package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.graph_api._

object ComputeVertexNeighborhoodFromTriplets {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    // The list of outgoing edges.
    val srcEdges = vertexAttribute[Array[ID]](vertices)
    // The list of incoming edges.
    val dstEdges = vertexAttribute[Array[ID]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val neighborhood = scalar[Set[ID]]
  }
}
import ComputeVertexNeighborhoodFromTriplets._
case class ComputeVertexNeighborhoodFromTriplets(
    centers: Seq[ID],
    radius: Int) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val vs = inputs.vertices.rdd
    val src = inputs.srcEdges.rdd
    val dst = inputs.srcEdges.rdd
    val all = src.fullOuterJoin(dst)
    var neigborhood = centers.toSet
    for (i <- 0 until radius) {
      neigborhood ++= all
        .restrictToIdSet(neigborhood.toIndexedSeq.sorted)
        .map { case (id, (src, dst)) => src.toSet.flatten ++ dst.toSet.flatten }
        .reduce(_ ++ _)
    }
    output(o.neighborhood, neigborhood)
  }
}
