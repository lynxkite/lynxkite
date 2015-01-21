package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object RestrictAttributeToIds extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vs = vertexSet
    val attr = vertexAttribute[T](vs)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: Input[T])
      extends MagicOutput(instance) {
    implicit val tt = inputs.attr.typeTag
    val attrMap = scalar[Map[ID, T]]
  }
  def run[T](attr: Attribute[T], ids: Set[ID])(
    implicit manager: MetaGraphManager): Scalar[Map[ID, T]] = {

    import Scripting._
    val op = RestrictAttributeToIds[T](ids)
    op(op.attr, attr).result.attrMap
  }
  def fromJson(j: play.api.libs.json.JsValue) = RestrictAttributeToIds[Any]((j \ "vertexIdSet").as[Set[ID]])
}
import RestrictAttributeToIds._
case class RestrictAttributeToIds[T](vertexIdSet: Set[ID])
    extends TypedMetaGraphOp[Input[T], Output[T]] {
  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val restricted = inputs.attr.rdd.restrictToIdSet(vertexIdSet.toIndexedSeq.sorted)
    output(o.attrMap, restricted.collect.toMap)
  }
}
