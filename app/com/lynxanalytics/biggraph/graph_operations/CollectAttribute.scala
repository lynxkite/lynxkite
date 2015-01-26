package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

// Collects the attribute values into a scalar Map for a small ID set.
object CollectAttribute extends OpFromJson {
  class Output[T](
      implicit instance: MetaGraphOperationInstance,
      inputs: VertexAttributeInput[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.attr.typeTag
    val idToAttr = scalar[Map[ID, T]]
  }
  def fromJson(j: play.api.libs.json.JsValue) = CollectAttribute((j \ "idSet").as[Set[ID]], (j \ "maxCount").as[Int])
}
import CollectAttribute._
case class CollectAttribute[T](
    idSet: Set[ID],
    maxCount: Int = 1000) extends TypedMetaGraphOp[VertexAttributeInput[T], Output[T]] {
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("idSet" -> idSet, "maxCount" -> maxCount)

  def execute(inputDatas: DataSet, o: Output[T], output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val ids = idSet.toIndexedSeq.sorted.take(maxCount)
    val restricted = inputs.attr.rdd.restrictToIdSet(ids)
    output(o.idToAttr, restricted.collect.toMap)
  }
}
