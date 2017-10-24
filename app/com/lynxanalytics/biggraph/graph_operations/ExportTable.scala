package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import org.apache.spark

object ExportTable {
  class Input extends MagicInputSignature {
    val t = table
  }

  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val exportResult = scalar[String]
  }
}

import ExportTable._
abstract class ExportTable extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
    inputDatas: DataSet,
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
  val quoteAllParameter = NewParameter("quoteAll", false)
  val escapeParameter = NewParameter("escape", "\\")
  val nullValueParameter = NewParameter("nullValue", "")
  val dateFormatParameter = NewParameter("dateFormat", "yyyy-MM-dd")
  val timestampFormatParameter = NewParameter("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  val dropLeadingWhiteSpaceParameter = NewParameter("dropLeadingWhiteSpace", false)
  val dropTrailingWhiteSpaceParameter = NewParameter("dropTrailingWhiteSpace", false)
  def fromJson(j: JsValue) = ExportTableToCSV(
    (j \ "path").as[String], (j \ "header").as[Boolean],
    (j \ "delimiter").as[String], (j \ "quote").as[String],
    quoteAllParameter.fromJson(j),
    escapeParameter.fromJson(j),
    nullValueParameter.fromJson(j),
    dateFormatParameter.fromJson(j),
    timestampFormatParameter.fromJson(j),
    dropLeadingWhiteSpaceParameter.fromJson(j),
    dropTrailingWhiteSpaceParameter.fromJson(j),
    (j \ "version").as[Int])
}

case class ExportTableToCSV(path: String, header: Boolean,
    delimiter: String, quote: String, quoteAll: Boolean,
    escape: String, nullValue: String, dateFormat: String, timestampFormat: String,
    dropLeadingWhiteSpace: Boolean, dropTrailingWhiteSpace: Boolean,
    version: Int)
  extends ExportTable {
  override def toJson = Json.obj(
    "path" -> path, "header" -> header,
    "delimiter" -> delimiter, "quote" -> quote,
    "version" -> version) ++
    ExportTableToCSV.quoteAllParameter.toJson(quoteAll) ++
    ExportTableToCSV.escapeParameter.toJson(escape) ++
    ExportTableToCSV.nullValueParameter.toJson(nullValue) ++
    ExportTableToCSV.dateFormatParameter.toJson(dateFormat) ++
    ExportTableToCSV.timestampFormatParameter.toJson(timestampFormat) ++
    ExportTableToCSV.dropLeadingWhiteSpaceParameter.toJson(dropLeadingWhiteSpace) ++
    ExportTableToCSV.dropTrailingWhiteSpaceParameter.toJson(dropTrailingWhiteSpace)

  def exportDataFrame(df: spark.sql.DataFrame) = {
    val file = HadoopFile(path)
    val options = Map(
      "sep" -> delimiter,
      "quote" -> quote,
      "quoteAll" -> (if (quoteAll) "true" else "false"),
      "escape" -> escape,
      "nullValue" -> nullValue,
      "dateFormat" -> dateFormat,
      "timestampFormat" -> timestampFormat,
      "ignoreLeadingWhiteSpace" -> (if (dropLeadingWhiteSpace) "true" else "false"),
      "ignoreTrailingWhiteSpaces" -> (if (dropTrailingWhiteSpace) "true" else "false"),
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
