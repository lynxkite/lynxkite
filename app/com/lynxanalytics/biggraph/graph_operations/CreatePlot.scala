// CreatePlot creates a vega-lite plot description using a table as input
// and a (vegas) scala code to specify the plot.
// The result of the operation ais a JSON description of the plot as a Scalar.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.sandbox.ScalaScript

object CreatePlot extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val plot = scalar[String]
  }
  def fromJson(j: JsValue) = CreatePlot(
    (j \ "plotCode").as[String],
    (j \ "title").as[String],
    (j \ "width").as[Int],
    (j \ "height").as[Int])
}

import CreatePlot._
case class CreatePlot(plotCode: String, title: String, width: Int, height: Int)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "plotCode" -> plotCode,
    "title" -> title,
    "width" -> width,
    "height" -> height)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val df = inputs.t.df
    val code = plotCode.replaceAll("\\s", "")
    val plotDescription: String = ScalaScript.runVegas(
      code, df, title = title, width = width, height = height)
    output(o.plot, plotDescription)
  }
}
