package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe.TypeTag

case class TableContents(
    header: List[(String, TypeTag[Any])],
    data: List[List[DynamicValue]])

object TableToScalar extends OpFromJson {
  override def fromJson(j: JsValue) = new TableToScalar((j \ "maxRows").as[Int])
  class Input extends MagicInputSignature {
    val tab = table
  }

  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val tableContents = scalar[TableContents]
  }

  def run(tab: Table, maxRows: Int = -1)(implicit m: MetaGraphManager) = {
    import Scripting._
    val op = TableToScalar(maxRows)
    op(op.tab, tab).result.tableContents
  }
}

import TableToScalar._
case class TableToScalar(maxRows: Int) extends SparkOperation[Input, Output] {
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
      if (maxRows < 0) rdd.collect().map { row => zipper(row) }
      else rdd.take(maxRows).map { row => zipper(row) }
    output(o.tableContents, TableContents(header, data.toList))
  }
}
