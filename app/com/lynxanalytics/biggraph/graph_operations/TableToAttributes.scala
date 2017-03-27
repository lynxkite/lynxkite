// Turns a Table into a VertexSet and its Attributes.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SQLHelper

object TableToAttributes extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  def fromJson(j: JsValue) = TableToAttributes()

  def run(table: Table)(implicit m: MetaGraphManager): SQLHelper.DataFrameOutput = {
    import Scripting._
    val op = TableToAttributes()
    op(op.t, table).result
  }
}
import TableToAttributes._
case class TableToAttributes() extends TypedMetaGraphOp[Input, SQLHelper.DataFrameOutput] {
  override val isHeavy = true
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    val schema = inputs.t.entity.schema
    new SQLHelper.DataFrameOutput(schema)(instance)
  }

  def execute(inputDatas: DataSet,
              o: SQLHelper.DataFrameOutput,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val t = inputs.t.data
    assert(t.entity.schema == t.df.schema,
      s"Schema mismatch: ${t.entity.schema} != ${t.df.schema}")
    o.populateOutput(rc, t.df.schema, t.df)
  }
}
