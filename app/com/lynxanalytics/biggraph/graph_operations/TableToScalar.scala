package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SQLHelper

import scala.reflect.runtime.universe.TypeTag

object TableToScalar extends OpFromJson {
  override def fromJson(j: JsValue) = new TableToScalar()
  class Input extends MagicInputSignature {
    val tab = table
  }

  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val header = scalar[List[(String, String)]]
    val data = scalar[List[List[DynamicValue]]]
  }

  def run(tab: Table): Unit = {
    import Scripting._
    val op = TableToScalar()

  }
}

import TableToScalar._
case class TableToScalar() extends SparkOperation[Input, Output] {
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  @transient override lazy val inputs = new Input()
  override val isHeavy: Boolean = false
  override def toJson = Json.obj()

  def execute(
    inputDatas: DataSet,
    o: Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    implicit val sd = rc.sparkDomain
    val df = inputs.tab.data.df
    val columns = df.schema.toList.map { field =>
      field.name -> SQLHelper.typeTagFromDataType(field.dataType).asInstanceOf[TypeTag[Any]]
    }
    output(o.data, List(List()))
    output(o.header, List())
  }
}
