// Zero-copy import. Reads a Parquet file outside of LynxKite, for which we know the schema.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.spark_util.SQLHelper

import org.apache.spark.sql.types

object ReadParquetWithSchema extends OpFromJson {
  def fromJson(j: JsValue) = ReadParquetWithSchema(
    (j \ "filename").as[String],
    ImportDataFrame.schemaFromJson(j \ "schema"),
  )

  def parseSchema(strings: Seq[String]): types.StructType = {
    val re = raw"\s*(\S+)\s*:\s*(\S+)\s*".r
    types.StructType(strings.map {
      case re(name, tpe) => types.StructField(
          name = name,
          dataType = SQLHelper.typeTagToDataType(SerializableType.TypeParser.parse(tpe).typeTag))
      case x => throw new AssertionError(s"The schema must be listed as 'column: type'. Got '$x'.")
    })
  }

  def run(filename: String, schema: Seq[String])(implicit mm: MetaGraphManager): Table = {
    import Scripting._
    new ReadParquetWithSchema(filename, parseSchema(schema))().result.t
  }
}

case class ReadParquetWithSchema(
    filename: String,
    schema: types.StructType)
    extends SparkOperation[NoInput, TableOutput] {
  override val isHeavy = true
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new TableOutput(schema)(instance)
  override def toJson = Json.obj(
    "filename" -> filename,
    "schema" -> schema.prettyJson,
  )

  def execute(
      inputDatas: DataSet,
      o: TableOutput,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    val f = HadoopFile(filename)
    val df = rc.sparkDomain.sparkSession.read.schema(schema).parquet(f.resolvedName)
    output(o.t, df)
  }
}
