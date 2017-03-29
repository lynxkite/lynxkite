// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark.sql

/* #5817
object ExecuteSQL extends OpFromJson {
  class Input(inputTables: Map[String, Seq[String]]) extends MagicInputSignature {
    val tables = inputTables.map {
      case (tableName, _) =>
        (tableName, vertexSet(Symbol(s"${tableName}_vs")))
    }.toMap

    val tableColumns = inputTables.map {
      case (tableName, columnNames) =>
        val columns = columnNames.map(
          columnName => (
            columnName,
            anyVertexAttribute(
              tables(tableName),
              Symbol(s"${tableName}_${columnName}_attr")
            )
          )
        )
        (tableName, columns.toMap)
    }.toMap
  }

  def fromJson(j: JsValue) = {
    new ExecuteSQL(
      (j \ "sqlQuery").as[String],
      (j \ "inputTables").as[Map[String, Seq[String]]],
      sql.types.DataType.fromJson((j \ "outputSchema").as[String])
        .asInstanceOf[sql.types.StructType]
    )
  }
}

case class ExecuteSQL(
  val sqlQuery: String,
  val inputTables: Map[String, Seq[String]],
  val outputSchema: sql.types.StructType)
    extends TypedMetaGraphOp[ExecuteSQL.Input, SQLHelper.DataFrameOutput] {

  import ExecuteSQL._
  override val isHeavy = true
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new Input(inputTables)

  def outputMeta(instance: MetaGraphOperationInstance) =
    new SQLHelper.DataFrameOutput(outputSchema)(instance)
  override def toJson = Json.obj(
    "sqlQuery" -> sqlQuery,
    "inputTables" -> inputTables,
    "outputSchema" -> outputSchema.prettyJson)

  def execute(inputDatas: DataSet,
              o: SQLHelper.DataFrameOutput,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.dataManager.newSQLContext()
    val dfs = inputs.tables.map {
      case (tableName, tableVs) =>
        val rawTable = RawTable(
          tableVs.entity(output.instance),
          inputs.tableColumns(tableName).mapValues(_.entity(output.instance)))
        val tableRelation =
          new RDDRelation(
            rawTable, sqlContext, tableVs.rdd, inputs.tableColumns(tableName).mapValues(_.rdd))
        tableName -> tableRelation.toDF
    }
    val dataFrame = DataManager.sql(sqlContext, sqlQuery, dfs.toList)
    o.populateOutput(rc, outputSchema, dataFrame)
  }
}
*/
