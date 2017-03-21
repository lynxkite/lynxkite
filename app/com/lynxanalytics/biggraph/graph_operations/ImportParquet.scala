// Operation for importing data from an internal Parquet file. These files are considered immutable.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.spark_util.SQLHelper

import org.apache.spark.sql.types
import play.api.libs.json

case class ParquetMetadata(
  filename: String,
  schema: types.StructType)

object ImportFromParquet extends OpFromJson {
  implicit object ParquetMetadataFormat extends json.Format[ParquetMetadata] {
    def writes(pm: ParquetMetadata): JsValue =
      json.Json.obj("schema" -> pm.schema.prettyJson, "filename" -> pm.filename)
    def reads(j: JsValue): json.JsResult[ParquetMetadata] = {
      val schema = types.DataType.fromJson((j \ "schema").as[String]).asInstanceOf[types.StructType]
      val filename = (j \ "filename").as[String]
      json.JsSuccess(ParquetMetadata(filename, schema))
    }
  }
  def fromJson(j: JsValue) = ImportFromParquet((j \ "parquetMetadata").as[ParquetMetadata])
}

case class ImportFromParquet(
    pm: ParquetMetadata) extends TypedMetaGraphOp[NoInput, SQLHelper.DataFrameOutput] {

  override val isHeavy = true
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) =
    new SQLHelper.DataFrameOutput(pm.schema)(instance)
  import ImportFromParquet.ParquetMetadataFormat
  override def toJson = Json.obj("parquetMetadata" -> pm)

  def execute(inputDatas: DataSet,
              o: SQLHelper.DataFrameOutput,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val f = HadoopFile(pm.filename)
    val df = rc.sqlContext.read.parquet(f.resolvedName)
    o.populateOutput(rc, pm.schema, df)
  }
}
