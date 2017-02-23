// Finds all vertices within a given distance from a set of vertices
// using per-vertex neighborhoods as input.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import scala.util.Sorting

object ComputeVertexNeighborhoodFromTriplets extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices) // We don't need this anymore, but have to keep here for legacy reasons?
    // The list of outgoing neighbors.
    val srcTripletMapping = vertexAttribute[Array[ID]](vertices)
    // The list of incoming neighbors.
    val dstTripletMapping = vertexAttribute[Array[ID]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val neighborhood = scalar[Set[ID]]
  }
  def fromJson(j: JsValue) =
    ComputeVertexNeighborhoodFromTriplets((j \ "centers").as[Seq[ID]], (j \ "radius").as[Int], (j \ "maxCount").as[Int])
}
import ComputeVertexNeighborhoodFromTriplets._
case class ComputeVertexNeighborhoodFromTriplets(
    centers: Seq[ID],
    radius: Int,
    // Maximal number of vertices to return. If the specified neighborhood is larger then this, then
    // the output will be empty set to signal this outcome.
    maxCount: Int) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj("centers" -> centers, "radius" -> radius, "maxCount" -> maxCount)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val all = inputs.srcTripletMapping.rdd.fullOuterJoin(inputs.dstTripletMapping.rdd)
    var neighborhood = centers.toArray
    var tooMuch = false
    for (i <- 0 until radius) {
      if (!tooMuch) {
        Sorting.quickSort(neighborhood)
        neighborhood = all
          .restrictToIdSet(neighborhood)
          .flatMap { case (id, (srcNeighbor, dstNeighbor)) => (srcNeighbor ++ dstNeighbor).flatten }
          .distinct
          .take(maxCount + 1)
        if (neighborhood.size > maxCount) {
          tooMuch = true
          neighborhood = Array()
        }
      }
    }
    // Isolated points are lost in the above loop. Add back centers to make sure they are present.
    val res = neighborhood.toSet ++ centers
    if (tooMuch || res.size > maxCount) {
      output(o.neighborhood, Set[ID]())
    } else {
      output(o.neighborhood, res)
    }
  }
}
