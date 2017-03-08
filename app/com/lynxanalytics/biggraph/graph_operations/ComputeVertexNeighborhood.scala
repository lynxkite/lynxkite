// Finds all vertices within a given distance from a set of vertices.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object ComputeVertexNeighborhood extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val neighborhood = scalar[Set[ID]]
  }
  def fromJson(j: JsValue) =
    ComputeVertexNeighborhood((j \ "centers").as[Seq[ID]], (j \ "radius").as[Int])
}
import ComputeVertexNeighborhood._
@deprecated("ComputeVertexNeighborhood is deprecated, use ComputeVertexNeighborhoodFromEdgesAndNeighbors", "1.13.0")
case class ComputeVertexNeighborhood(
    centers: Seq[ID],
    radius: Int) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj("centers" -> centers, "radius" -> radius)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val es = inputs.edges.rdd
    var neighborhood = centers.toSet
    for (i <- 0 until radius) {
      neighborhood ++= es
        .values
        .filter(e => (neighborhood.contains(e.src) != neighborhood.contains(e.dst)))
        .flatMap(e => Iterator(e.src, e.dst))
        .distinct
        .collect
        .toSet
    }
    output(o.neighborhood, neighborhood)
  }
}
