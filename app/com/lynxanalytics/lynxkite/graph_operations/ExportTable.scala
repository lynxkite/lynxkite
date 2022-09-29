package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_util.HadoopFile
import org.apache.spark
import org.apache.spark.sql.SaveMode

object ExportTable {
  class Input extends MagicInputSignature {
    val t = table
  }

  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val exportResult = scalar[String]
  }

  def toSaveMode(saveMode: String) = saveMode match {
    case "error if exists" => SaveMode.ErrorIfExists
    case "overwrite" => SaveMode.Overwrite
    case "append" => SaveMode.Append
    case "ignore" => SaveMode.Ignore
    case _ => throw new AssertionError(s"Invalid save mode: $saveMode")
  }

  def maybeRepartitionForDownload(df: spark.sql.DataFrame, forDownload: Boolean): spark.sql.DataFrame = {
    if (!forDownload || df.rdd.getNumPartitions == 1) df
    else df.repartition(1)
  }
}

import ExportTable._
abstract class ExportTable extends SparkOperation[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    implicit val sd = rc.sparkDomain
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
  val saveModeParameter = NewParameter("save_mode", "error if exists")
  val forDownLoadParameter = NewParameter[Boolean]("for_download", false)
  def fromJson(j: JsValue) = ExportTableToCSV(
    (j \ "path").as[String],
    (j \ "header").as[Boolean],
    (j \ "delimiter").as[String],
    (j \ "quote").as[String],
    quoteAllParameter.fromJson(j),
    escapeParameter.fromJson(j),
    nullValueParameter.fromJson(j),
    dateFormatParameter.fromJson(j),
    timestampFormatParameter.fromJson(j),
    dropLeadingWhiteSpaceParameter.fromJson(j),
    dropTrailingWhiteSpaceParameter.fromJson(j),
    (j \ "version").as[Int],
    saveModeParameter.fromJson(j),
    forDownLoadParameter.fromJson(j),
  )
}

case class ExportTableToCSV(
    path: String,
    header: Boolean,
    delimiter: String,
    quote: String,
    quoteAll: Boolean,
    escape: String,
    nullValue: String,
    dateFormat: String,
    timestampFormat: String,
    dropLeadingWhiteSpace: Boolean,
    dropTrailingWhiteSpace: Boolean,
    version: Int,
    saveMode: String,
    forDownload: Boolean)
    extends ExportTable {
  override def toJson = Json.obj(
    "path" -> path,
    "header" -> header,
    "delimiter" -> delimiter,
    "quote" -> quote,
    "version" -> version) ++
    ExportTableToCSV.quoteAllParameter.toJson(quoteAll) ++
    ExportTableToCSV.escapeParameter.toJson(escape) ++
    ExportTableToCSV.nullValueParameter.toJson(nullValue) ++
    ExportTableToCSV.dateFormatParameter.toJson(dateFormat) ++
    ExportTableToCSV.timestampFormatParameter.toJson(timestampFormat) ++
    ExportTableToCSV.dropLeadingWhiteSpaceParameter.toJson(dropLeadingWhiteSpace) ++
    ExportTableToCSV.dropTrailingWhiteSpaceParameter.toJson(dropTrailingWhiteSpace) ++
    ExportTableToCSV.saveModeParameter.toJson(saveMode) ++
    ExportTableToCSV.forDownLoadParameter.toJson(forDownload)

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
      "header" -> (if (header) "true" else "false"),
    )
    val mode = toSaveMode(saveMode)
    maybeRepartitionForDownload(df, forDownload).write.mode(mode).format("csv").options(options).save(file.resolvedName)
  }
}

object ExportTableToStructuredFile extends OpFromJson {
  val saveModeParameter = NewParameter("save_mode", "error if exists")
  val forDownLoadParameter = NewParameter[Boolean]("for_download", false)
  def fromJson(j: JsValue) = ExportTableToStructuredFile(
    (j \ "path").as[String],
    (j \ "format").as[String],
    (j \ "version").as[Long],
    saveModeParameter.fromJson(j),
    forDownLoadParameter.fromJson(j))
}

case class ExportTableToStructuredFile(
    path: String,
    format: String,
    version: Long,
    saveMode: String,
    forDownload: Boolean)
    extends ExportTable {

  override def toJson = Json.obj(
    "path" -> path,
    "format" -> format,
    "version" -> version) ++
    ExportTableToStructuredFile.saveModeParameter.toJson(saveMode) ++
    ExportTableToStructuredFile.forDownLoadParameter.toJson(forDownload)

  def exportDataFrame(df: spark.sql.DataFrame) = {
    val file = HadoopFile(path)
    val mode = toSaveMode(saveMode)
    maybeRepartitionForDownload(df, forDownload).write.mode(mode).format(format).save(file.resolvedName)
  }
}

object ExportTableToJdbc extends OpFromJson {
  def fromJson(j: JsValue) = ExportTableToJdbc(
    (j \ "jdbc_url").as[String],
    (j \ "jdbc_table").as[String],
    (j \ "mode").as[String])
}

case class ExportTableToJdbc(jdbcUrl: String, table: String, mode: String)
    extends ExportTable {

  override def toJson = Json.obj("jdbc_url" -> jdbcUrl, "jdbc_table" -> table, "mode" -> mode)

  def exportDataFrame(df: spark.sql.DataFrame) = {
    df.write.mode(mode).jdbc(jdbcUrl, table, new java.util.Properties)
  }
}

object ExportTableToHive extends OpFromJson {
  def fromJson(j: JsValue) = ExportTableToHive(
    (j \ "table").as[String],
    (j \ "mode").as[String],
    (j \ "partitionBy").as[Seq[String]])
}

case class ExportTableToHive(table: String, mode: String, partitionBy: Seq[String])
    extends ExportTable {

  override def toJson = Json.obj("table" -> table, "mode" -> mode, "partitionBy" -> partitionBy)

  def exportDataFrame(df: spark.sql.DataFrame) = {
    if (partitionBy.isEmpty) {
      df.write.mode(mode).saveAsTable(table)
    } else {
      assert(mode != "overwrite", "overwrite mode cannot be used with partition columns")
      df.write.mode(mode).partitionBy(partitionBy: _*).saveAsTable(table)
    }
  }
}
