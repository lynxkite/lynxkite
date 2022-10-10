// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.spark_util.SQLHelper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types

object ImportDataFrame extends OpFromJson {

  def schemaFromJson(j: play.api.libs.json.JsLookupResult): types.StructType = {
    // This is meta level, so we may not have a Spark session at this point.
    // But we've got to allow reading old schemas for compatibility.
    org.apache.spark.sql.internal.SQLConf.get.setConfString("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
    types.DataType.fromJson(j.as[String]).asInstanceOf[types.StructType]
  }

  def fromJson(j: JsValue) = {
    new ImportDataFrame(schemaFromJson(j \ "schema"), None, (j \ "timestamp").as[String])
  }

  private def apply(df: DataFrame) = {
    new ImportDataFrame(SQLHelper.stripComment(df.schema), Some(df), Timestamp.toString)
  }

  def run(df: DataFrame)(implicit mm: MetaGraphManager): Table = {
    import Scripting._
    ImportDataFrame(df)().result.t
  }

  class Output(schema: types.StructType)(
      implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    val t = table(schema)
  }
}

// We cannot serialize the DataFrame. For persistence purposes the timestamp will be used to
// identify this operation and generate its GUID. This means the operation cannot be executed
// when it's loaded from disk. It has to be executed when it's first created. If that succeeds,
// the data will be stored on disk and we will never again try to execute it.
class ImportDataFrame private (
    val schema: types.StructType,
    inputFrame: Option[DataFrame],
    val timestamp: String)
    extends SparkOperation[NoInput, ImportDataFrame.Output] {

  for (df <- inputFrame) {
    // If the DataFrame is backed by LynxKite operations, we need to trigger these now. Triggering
    // them inside execute() could lead to SparkDomain thread exhaustion. #5580
    df.rdd
  }

  override def equals(other: Any): Boolean =
    other match {
      case otherOp: ImportDataFrame =>
        (otherOp.schema == schema) && (otherOp.timestamp == timestamp)
      case _ => false
    }
  override lazy val hashCode = gUID.hashCode
  override val isHeavy = true
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) =
    new ImportDataFrame.Output(schema)(instance)
  override def toJson = Json.obj(
    "schema" -> schema.prettyJson,
    "timestamp" -> timestamp)

  def execute(
      inputDatas: DataSet,
      o: ImportDataFrame.Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    inputFrame match {
      case None =>
        throw new AssertionError(
          "Import failed or imported data have been lost (if this table was successfully imported" +
            " before then contact your system administrator)")
      case Some(df) =>
        output(o.t, df)
    }
  }
}
