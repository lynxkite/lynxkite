// Collects the attribute values into a scalar Map for a small ID set.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object CollectAttribute extends OpFromJson {
  class Output[T](
      implicit
      instance: MetaGraphOperationInstance,
      inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    implicit val tt = inputs.attr.typeTag
    val idToAttr = scalar[Map[ID, T]]
  }
  def fromJson(j: JsValue) = CollectAttribute((j \ "idSet").as[Set[ID]])
}
import CollectAttribute._
case class CollectAttribute[T](
    idSet: Set[ID])
    extends SparkOperation[VertexAttributeInput[T], Output[T]] {
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("idSet" -> idSet)

  def execute(inputDatas: DataSet, o: Output[T], output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val ids = idSet.toIndexedSeq.sorted
    val restricted = inputs.attr.rdd.restrictToIdSet(ids)
    output(o.idToAttr, restricted.collect.toMap)
  }
}
