package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import org.apache.spark

object ExportTable {
  class Input extends MagicInputSignature {
    val t = table
  }

  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val exportResult = scalar[String]
  }
}

import ExportTable._
abstract class ExportTable extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    implicit val dataManager = rc.dataManager
    val df = inputs.t.df
    exportDataFrame(df)
    val exportResult = "Export done."
    output(o.exportResult, exportResult)
  }

  def exportDataFrame(df: spark.sql.DataFrame)
}

object ExportTableToCSV extends OpFromJson {
  def fromJson(j: JsValue) = ExportTableToCSV(
    (j \ "path").as[String], (j \ "header").as[Boolean],
    (j \ "delimiter").as[String], (j \ "quote").as[String],
    (j \ "version").as[Int])
}

case class ExportTableToCSV(path: String, header: Boolean,
                            delimiter: String, quote: String, version: Int)
    extends ExportTable {
  override def toJson = Json.obj(
    "path" -> path, "header" -> header,
    "delimiter" -> delimiter, "quote" -> quote, "version" -> version)

  def exportDataFrame(df: spark.sql.DataFrame) = {
    val file = HadoopFile(path)
    val options = Map(
      "delimiter" -> delimiter,
      "quote" -> quote,
      "nullValue" -> "",
      "header" -> (if (header) "true" else "false"))
    df.write.format("csv").options(options).save(file.resolvedName)
  }
}

object ExportTableToStructuredFile extends OpFromJson {
  def fromJson(j: JsValue) = ExportTableToStructuredFile(
    (j \ "path").as[String], (j \ "format").as[String],
    (j \ "version").as[Int])
}

case class ExportTableToStructuredFile(path: String, format: String, version: Int)
    extends ExportTable {

  override def toJson = Json.obj(
    "path" -> path, "format" -> format, "version" -> version)

  def exportDataFrame(df: spark.sql.DataFrame) = {
    val file = HadoopFile(path)
    df.write.format(format).save(file.resolvedName)
  }
}

object ExportTableToJdbc extends OpFromJson {
  def fromJson(j: JsValue) = ExportTableToJdbc(
    (j \ "jdbc_url").as[String], (j \ "jdbc_table").as[String], (j \ "mode").as[String])
}

case class ExportTableToJdbc(jdbcUrl: String, table: String, mode: String)
    extends ExportTable {

  override def toJson = Json.obj("jdbc_url" -> jdbcUrl, "jdbc_table" -> table, "mode" -> mode)

  def exportDataFrame(df: spark.sql.DataFrame) = {
    df.write.mode(mode).jdbc(jdbcUrl, table, new java.util.Properties)
  }
}
