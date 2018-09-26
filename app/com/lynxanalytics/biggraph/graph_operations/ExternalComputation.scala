package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object ExternalComputation extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }

  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val t = table(inputs.t.entity.schema)
  }

  def fromJson(j: JsValue) = ExternalComputation((j \ "label").as[String])
}

import ExternalComputation._
case class ExternalComputation(label: String) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("label" -> label)

  def execute(
    inputDatas: DataSet,
    o: Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    throw new AssertionError(s"External computation '$label' must be executed externally.")
  }
}
