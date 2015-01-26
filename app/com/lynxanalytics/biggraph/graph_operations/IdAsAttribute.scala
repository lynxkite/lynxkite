package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object IdAsAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val vertexIds = vertexAttribute[ID](inputs.vertices.entity)
  }
  def run(vs: VertexSet)(implicit manager: MetaGraphManager): Attribute[ID] = {
    val op = IdAsAttribute()
    import Scripting._
    op(op.vertices, vs).result.vertexIds
  }
  def fromJson(j: JsValue) = IdAsAttribute()
}
import IdAsAttribute._
case class IdAsAttribute() extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    output(
      o.vertexIds,
      inputs.vertices.rdd.mapValuesWithKeys(_._1))
  }
}
