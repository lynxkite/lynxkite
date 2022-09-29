// Operation to create a scalar that contains the contents (top k rows)
// of a dataframe (table) to be used by the frontend (e.g., displaying
// some of it.
package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.SQLHelper

import scala.reflect.runtime.universe.TypeTag

case class TableContents(
    header: List[(String, TypeTag[Any])],
    rows: List[List[DynamicValue]])

object TableToScalar extends OpFromJson {
  override def fromJson(j: JsValue) = new TableToScalar((j \ "maxRows").as[Int])
  class Input extends MagicInputSignature {
    val tab = table
  }

  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val tableContents = scalar[TableContents]
  }

  def run(tab: Table, maxRows: Int)(implicit m: MetaGraphManager): Scalar[TableContents] = {
    import Scripting._
    val op = TableToScalar(maxRows)
    op(op.tab, tab).result.tableContents
  }
}

import TableToScalar._
case class TableToScalar private (maxRows: Int) extends SparkOperation[Input, Output] {
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  @transient override lazy val inputs = new Input()
  override val isHeavy: Boolean = false
  override def toJson = Json.obj("maxRows" -> maxRows)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    implicit val sd = rc.sparkDomain
    val df = inputs.tab.data.df
    val header = df.schema.toList.map { field =>
      field.name -> SQLHelper.typeTagFromDataType(field.dataType).asInstanceOf[TypeTag[Any]]
    }
    def zipper(row: Seq[Any]) = {
      row.toList.zip(header).map {
        case (null, _) => DynamicValue("null", defined = false)
        case (item, (_, tt)) => DynamicValue.convert(item)(tt)
      }
    }
    val rdd = SQLHelper.toSeqRDD(df)
    val data =
      if (maxRows < 0) rdd.collect().map(zipper)
      else rdd.take(maxRows).map(zipper)
    output(o.tableContents, TableContents(header, data.toList))
  }
}
