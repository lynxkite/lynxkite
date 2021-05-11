// CreatePlot used to create Vega-Lite's JSON from a Vegas (Scala) code.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

@deprecated("Write Vega-lite JSON directly.", "4.3.0")
object CreatePlot extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val plot = scalar[String]
  }
  def fromJson(j: JsValue) = CreatePlot((j \ "plotCode").as[String])
}

import CreatePlot._
@deprecated("Write Vega-lite JSON directly.", "4.3.0")
case class CreatePlot(plotCode: String) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("plotCode" -> plotCode)
}
