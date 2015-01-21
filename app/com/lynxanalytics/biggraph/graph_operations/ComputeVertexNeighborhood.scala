package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.graph_api._

object ComputeVertexNeighborhood extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val neighborhood = scalar[Set[ID]]
  }
  def fromJson(j: play.api.libs.json.JsValue) =
    ComputeVertexNeighborhood((j \ "centers").as[Seq[ID]], (j \ "radius").as[Int])
}
import ComputeVertexNeighborhood._
case class ComputeVertexNeighborhood(
    centers: Seq[ID],
    radius: Int) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = play.api.libs.json.Json.obj("centers" -> centers, "radius" -> radius)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val vs = inputs.vertices.rdd
    val es = inputs.edges.rdd
    val vsPart = vs.partitioner.get
    var neigborhood = centers.toSet
    for (i <- 0 until radius) {
      neigborhood ++= es
        .values
        .filter(e => (neigborhood.contains(e.src) != neigborhood.contains(e.dst)))
        .flatMap(e => Iterator(e.src, e.dst))
        .distinct
        .collect
        .toSet
    }
    output(o.neighborhood, neigborhood)
  }
}
