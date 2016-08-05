// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.controllers.Table
import com.lynxanalytics.biggraph.controllers.RawTable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import com.lynxanalytics.biggraph.table.TableRelation

import org.apache.spark.sql
import org.apache.spark.sql.types
import org.apache.spark.sql.sources
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd

import scala.reflect.runtime.universe.TypeTag

import play.api.libs.json

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
        val tableColumnList: Iterable[(String, Attribute[_])] = inputs
          .tableColumns(tableName)
          .map {
            case (columnName, columnAttr) =>
              (columnName, columnAttr.entity(output.instance))
          }
        val rawTable = RawTable(tableVs.entity(output.instance), tableColumnList.toMap)
        val tableRelation = new TableRelation(rawTable, sqlContext)(rc.dataManager)
        tableName -> tableRelation.toDF
    }
    var dataFrame = DataManager.sqlWith(sqlContext, sqlQuery, dfs.toList)
    o.populateOutput(rc, outputSchema, dataFrame)
  }
}
