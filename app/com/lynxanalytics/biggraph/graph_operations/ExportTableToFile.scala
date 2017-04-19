package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.controllers.FileMetaData
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.{ HadoopFile, Timestamp }
import com.lynxanalytics.biggraph.serving.DownloadFileRequest

object ExportTableToFile {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val exportResult = scalar[FileMetaData]
  }

  def getFile(format: String, path: String)(implicit dataManager: DataManager) = {
    if (path == "<download>") {
      dataManager.repositoryPath / "exports" / Timestamp.toString + "." + format
    } else {
      HadoopFile(path)
    }
  }
}

import ExportTableToFile._
abstract class ExportTableToFile extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
}

object ExportTableToCSV extends OpFromJson {
  def fromJson(j: JsValue) = ExportTableToCSV(
    (j \ "path").as[String], (j \ "header").as[Boolean],
    (j \ "delimiter").as[String], (j \ "quote").as[String],
    (j \ "version").as[Int])
}

case class ExportTableToCSV(path: String, header: Boolean,
                            delimiter: String, quote: String, version: Int)
    extends ExportTableToFile {
  override def toJson = Json.obj(
    "path" -> path, "header" -> header,
    "delimiter" -> delimiter, "quote" -> quote, "version" -> version)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    implicit val dataManager = rc.dataManager
    val file = getFile("csv", path)
    val df = inputs.t.df
    val options = Map(
      "delimiter" -> delimiter,
      "quote" -> quote,
      "nullValue" -> "",
      "header" -> (if (header) "true" else "false"))
    df.write.format("csv").options(options).save(file.resolvedName)
    val numberOfRows = df.count
    val download =
      if (path == "<download>") Some(DownloadFileRequest(file.symbolicName, !header)) else None
    val exportResult = FileMetaData(numberOfRows, "csv", file.resolvedName, download)
    output(o.exportResult, exportResult)
  }
}

object ExportTableToStructuredFile extends OpFromJson {
  def fromJson(j: JsValue) = ExportTableToStructuredFile(
    (j \ "path").as[String], (j \ "format").as[String],
    (j \ "version").as[Int])
}

case class ExportTableToStructuredFile(path: String, format: String, version: Int)
    extends ExportTableToFile {

  override def toJson = Json.obj(
    "path" -> path, "format" -> format, "version" -> version)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    implicit val dataManager = rc.dataManager
    val file = getFile(format, path)
    val df = inputs.t.df
    df.write.format(format).save(file.resolvedName)
    val numberOfRows = df.count
    val download =
      if (path == "<download>") Some(DownloadFileRequest(file.symbolicName, false)) else None
    val exportResult = FileMetaData(numberOfRows, format, file.resolvedName, download)
    output(o.exportResult, exportResult)
  }
}
