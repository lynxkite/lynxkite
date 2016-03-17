// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.controllers.Table
import com.lynxanalytics.biggraph.controllers.RawTable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import com.lynxanalytics.biggraph.spark_util.SQLHelper

import org.apache.spark.sql
import org.apache.spark.sql.types
import org.apache.spark.sql.sources
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd

import scala.reflect.runtime.universe.TypeTag

import play.api.libs.json

class AttributeTableRelation(
  vs: UniqueSortedRDD[ID, Unit],
  attrs: Map[String, AttributeData[_]],
  val sqlContext: sql.SQLContext)
    extends sources.BaseRelation with sources.TableScan with sources.PrunedScan {

  // TableScan
  override def buildScan(): rdd.RDD[sql.Row] = buildScan(schema.fieldNames)

  // PrunedScan
  override def buildScan(requiredColumns: Array[String]): rdd.RDD[sql.Row] = {

    val rdds = requiredColumns.toSeq.map(name => attrs(name))
    val emptyRows = vs.mapValues(_ => Seq[Any]())
    val seqRows = rdds.foldLeft(emptyRows) {
      case (seqs, data) =>
        seqs
          .sortedLeftOuterJoin(data.rdd)
          .mapValues { case (seq, opt) => seq :+ opt.getOrElse(null) }
    }
    seqRows.values.map(sql.Row.fromSeq(_))
  }

  def schema = {
    val fields = attrs.toSeq.sortBy(_._1).map {
      case (name, data) =>
        types.StructField(
          name = name,
          dataType = Table.dfType(data.typeTag))
    }
    sql.types.StructType(fields)
  }

  def toDF = sqlContext.baseRelationToDataFrame(this)
}

object ExecuteSQL extends OpFromJson {
  def toSymbol(field: sql.types.StructField) = Symbol("imported_column_" + field.name)

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
            vertexAttribute[Any](
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

class ExecuteSQL(
  val sqlQuery: String,
  val inputTables: Map[String, Seq[String]],
  val outputSchema: sql.types.StructType)
    extends TypedMetaGraphOp[ExecuteSQL.Input, SQLHelper.DataFrameOutput] {

  override lazy val hashCode = gUID.hashCode

  // Needed because of outputSchema.
  override def equals(other: Any): Boolean =
    other match {
      case otherOp: ExecuteSQL =>
        (otherOp.outputSchema == outputSchema) && (otherOp.inputTables == inputTables) && (otherOp.sqlQuery == sqlQuery)
      case _ => false
    }

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

    val sqlContext = rc.sqlContext.newSession()
    for ((tableName, tableVs) <- inputs.tables) {
      val tc = inputs
        .tableColumns(tableName)
        .map {
          case (columnName, columnAttr) =>
            (columnName, columnAttr.data)
        }
        .toMap

      val rt = new AttributeTableRelation(tableVs.rdd, tc, sqlContext)
      val df = rt.toDF
      df.registerTempTable(tableName)
    }
    var dataFrame = sqlContext.sql(sqlQuery)
    o.populateOutput(rc, outputSchema, dataFrame)
  }
}
