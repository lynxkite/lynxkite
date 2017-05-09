// CreatePlot creates a vega-lite plot description using a table as input
// and a (vegas) scala code to specify the plot.
// The result of the operation ais a JSON description of the plot as a Scalar.
package com.lynxanalytics.biggraph.graph_operations

import vegas._
import vegas.sparkExt._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations

object CreatePlot extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val plot = scalar[String]
  }
  def fromJson(j: JsValue) = CreatePlot(
    (j \ "plotCode").as[String]
  )
}
import CreatePlot._
case class CreatePlot(plotCode: String)
  extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("plotCode" -> plotCode)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val df = inputs.t.df
    // create plot generating script

    // eval script

    // test
    val plotDescription: String = df.take(1).toString

    output(o.plot, plotDescription)
  }
}
