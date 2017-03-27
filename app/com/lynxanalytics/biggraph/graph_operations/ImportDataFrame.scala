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

  // Renames columns to names that are allowed by Parquet.
  private def normalize(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) {
      (df, c) => df.withColumnRenamed(c, normalizeColumnName(c))
    }
  }

  private def normalizeColumnName(name: String): String = {
    name.replaceAll("[ ,;{}()\n\t=]", "_")
  }

  def apply(df: DataFrame) = {
    val ndf = normalize(df)
    new ImportDataFrame(ndf.schema, Some(ndf), Timestamp.toString)
  }

  def run(df: DataFrame)(implicit mm: MetaGraphManager): Table = {
    import Scripting._
    ImportDataFrame(df)().result.t
  }

  class Output(schema: types.StructType)(
      implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val t = table(schema)
  }
}

class ImportDataFrame private (
  val schema: types.StructType,
  inputFrame: Option[DataFrame],
  val timestamp: String)
    extends TypedMetaGraphOp[NoInput, ImportDataFrame.Output] {

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
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) =
    new ImportDataFrame.Output(schema)(instance)
  override def toJson = Json.obj(
    "schema" -> schema.prettyJson,
    "timestamp" -> timestamp)

  def execute(inputDatas: DataSet,
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
