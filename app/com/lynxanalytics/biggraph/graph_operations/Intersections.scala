// The intersection of some number of vertex sets.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object VertexSetIntersection extends OpFromJson {
  class Input(numVertexSets: Int) extends MagicInputSignature {
    val vss = Range(0, numVertexSets).map {
      i => vertexSet(Symbol("vs" + i))
    }.toList
  }
  class Output(
      implicit instance: MetaGraphOperationInstance,
      input: Input) extends MagicOutput(instance) {

    val intersection = vertexSet
    // An embedding of the intersection into the first vertex set.
    val firstEmbedding = edgeBundle(
      intersection, input.vss(0).entity, EdgeBundleProperties.embedding)
  }
  def fromJson(j: JsValue) = VertexSetIntersection(
    (j \ "numVertexSets").as[Int],
    (j \ "heavy").as[Boolean])
}
case class VertexSetIntersection(numVertexSets: Int, heavy: Boolean = false)
    extends TypedMetaGraphOp[VertexSetIntersection.Input, VertexSetIntersection.Output] {

  import VertexSetIntersection._

  override val isHeavy = heavy

  assert(numVertexSets >= 1)
  @transient override lazy val inputs = new Input(numVertexSets)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("numVertexSets" -> numVertexSets, "heavy" -> heavy)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val intersection = {
      val intersection = inputs.vss.map(_.rdd) // All the rdds here must have the same number of
        // partitions; we do assert this in sortedJoin.
        .reduce((rdd1, rdd2) => rdd1.sortedJoin(rdd2).mapValues(_ => ()))
      // Repartition if heavy.
      if (heavy) {
        intersection.cache()
        val size = intersection.count
        intersection.toSortedRDD(rc.partitionerForNRows(size))
      } else {
        intersection
      }
    }
    output(o.intersection, intersection)
    output(o.firstEmbedding, intersection.mapValuesWithKeys { case (id, _) => Edge(id, id) })
  }
}
