// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.spark_util.SQLHelper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types

object ImportDataFrame extends OpFromJson {

  def fromJson(j: JsValue) = new ImportDataFrame(
    types.DataType.fromJson((j \ "schema").as[String]).asInstanceOf[types.StructType],
    None,
    (j \ "timestamp").as[String])

  def apply(inputFrame: DataFrame) =
    new ImportDataFrame(
      inputFrame.schema,
      Some(inputFrame),
      Timestamp.toString)
}

class ImportDataFrame private (
  val schema: types.StructType,
  inputFrame: Option[DataFrame],
  val timestamp: String)
    extends TypedMetaGraphOp[NoInput, SQLHelper.DataFrameOutput] {

  for (df <- inputFrame) {
    // If the DataFrame is backed by LynxKite operations, we need to trigger these now. Triggering
    // them inside execute() could lead to DataManager thread exhaustion. #5580
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
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new NoInput()

  def outputMeta(instance: MetaGraphOperationInstance) =
    new SQLHelper.DataFrameOutput(schema)(instance)
  override def toJson = Json.obj(
    "schema" -> schema.prettyJson,
    "timestamp" -> timestamp)

  def execute(inputDatas: DataSet,
              o: SQLHelper.DataFrameOutput,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    assert(
      inputFrame.nonEmpty,
      "Import failed or imported data have been lost (if this table was successfully imported" +
        " before then contact your system administrator)")
    o.populateOutput(rc, schema, inputFrame.get)
  }
}
