// A heavy operation that causes a table to be saved.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object SaveTable extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val t = table(inputs.t.entity.schema)
  }

  def fromJson(j: JsValue) = SaveTable()

  def run(table: Table)(implicit m: MetaGraphManager): Table = {
    import Scripting._
    val op = SaveTable()
    op(op.t, table).result.t
  }
}

import SaveTable._
case class SaveTable() extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.t, inputs.t.df)
  }
}
