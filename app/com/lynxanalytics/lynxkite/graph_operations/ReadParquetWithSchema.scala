// Zero-copy import. Reads a Parquet file outside of LynxKite, for which we know the schema.
package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_util.HadoopFile
import com.lynxanalytics.lynxkite.spark_util.SQLHelper

import org.apache.spark.sql.types

object ReadParquetWithSchema extends OpFromJson {
  def fromJson(j: JsValue) = ReadParquetWithSchema(
    (j \ "filename").as[String],
    ImportDataFrame.schemaFromJson(j \ "schema"),
  )

  def parseSchema(strings: Seq[String]): types.StructType = {
    val re = raw"\s*(.*?)\s*:\s*(.*?)\s*".r
    val reArray = raw"array of\s+(.*)".r
    types.StructType(strings.map {
      case re(name, tpe) => types.StructField(
          name = name,
          dataType = types.DataType.fromJson {
            tpe.toLowerCase match {
              case "array" => throw new AssertionError(
                  "For array types you need to specify the element type too. For example: 'array of string'")
              case reArray(t) =>
                s"""{"type": "array", "containsNull": true, "elementType": "$t"}"""
              case t => s""" "$t" """
            }
          },
        )
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
