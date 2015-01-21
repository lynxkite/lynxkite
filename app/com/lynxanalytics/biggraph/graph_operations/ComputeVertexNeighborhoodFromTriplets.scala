package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.graph_api._
import scala.util.Sorting

object ComputeVertexNeighborhoodFromTriplets extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices)
    // The list of outgoing edges.
    val srcTripletMapping = vertexAttribute[Array[ID]](vertices)
    // The list of incoming edges.
    val dstTripletMapping = vertexAttribute[Array[ID]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val neighborhood = scalar[Set[ID]]
  }
  def fromJson(j: play.api.libs.json.JsValue) = ComputeVertexNeighborhoodFromTriplets(Seq(), 0)
}
import ComputeVertexNeighborhoodFromTriplets._
case class ComputeVertexNeighborhoodFromTriplets(
    centers: Seq[ID],
    radius: Int) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val edges = inputs.edges.rdd
    val all = inputs.srcTripletMapping.rdd.fullOuterJoin(inputs.dstTripletMapping.rdd)
    var neighborhood = centers.toArray
    for (i <- 0 until radius) {
      Sorting.quickSort(neighborhood)
      val neighborEdges = all
        .restrictToIdSet(neighborhood.distinct)
        .flatMap { case (id, (srcEdge, dstEdge)) => (srcEdge ++ dstEdge).flatten }
        .collect
      Sorting.quickSort(neighborEdges)
      neighborhood = edges.restrictToIdSet(neighborEdges.distinct)
        .flatMap { case (id, edge) => Iterator(edge.src, edge.dst) }
        .collect
    }
    output(o.neighborhood, neighborhood.toSet ++ centers)
  }
}
