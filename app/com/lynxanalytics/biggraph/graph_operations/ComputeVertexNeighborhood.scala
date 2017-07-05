// Finds all vertices within a given distance from a set of vertices
// using per-vertex neighborhoods as input.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import scala.collection.mutable

object ComputeVertexNeighborhood extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    // The list of outgoing neighbors and edges.
    val srcMapping = vertexAttribute[EdgesAndNeighbors](vertices)
    // The list of incoming neighbors and edges.
    val dstMapping = vertexAttribute[EdgesAndNeighbors](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val neighborhood = scalar[Set[ID]]
  }
  def fromJson(j: JsValue) = ComputeVertexNeighborhood(
    (j \ "centers").as[Seq[ID]],
    (j \ "radius").as[Int],
    (j \ "maxCount").as[Int])
}
import ComputeVertexNeighborhood._
case class ComputeVertexNeighborhood(
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
    val all = inputs.srcMapping.rdd.fullOuterJoin(inputs.dstMapping.rdd)
    var neighborhood = mutable.SortedSet[ID]() ++ centers
    var tooMuch = neighborhood.size > maxCount
    for (i <- 0 until radius) {
      if (!tooMuch) {
        neighborhood ++= all
          .restrictToIdSet(neighborhood.toArray)
          .flatMap {
            case (_, (srcNeighbor, dstNeighbor)) =>
              (srcNeighbor.map(_.nids) ++ dstNeighbor.map(_.nids)).flatten
          }
          .distinct
          .take(maxCount + 1)
        if (neighborhood.size > maxCount) {
          tooMuch = true
          neighborhood = mutable.SortedSet[ID]()
        }
      }
    }
    if (tooMuch) {
      output(o.neighborhood, Set[ID]())
    } else {
      output(o.neighborhood, neighborhood.toSet)
    }
  }
}
