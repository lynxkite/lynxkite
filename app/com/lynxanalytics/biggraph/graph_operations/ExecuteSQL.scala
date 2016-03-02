// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.controllers.Table
import com.lynxanalytics.biggraph.controllers.RawTable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql
import org.apache.spark.rdd

import scala.reflect.runtime.universe.TypeTag

import play.api.libs.json

class AttributeTableRelation(
  vs: UniqueSortedRDD[ID, Unit],
  attrs: Map[String, (UniqueSortedRDD[ID, _], TypeTag[_])],
  val sqlContext: sql.SQLContext)
    extends sql.sources.BaseRelation with sql.sources.TableScan with sql.sources.PrunedScan {

  // TableScan
  override def buildScan(): rdd.RDD[sql.Row] = buildScan(schema.fieldNames)

  // PrunedScan
  override def buildScan(requiredColumns: Array[String]): rdd.RDD[sql.Row] = {

    val rdds = requiredColumns.toSeq.map(name => attrs(name))
    val emptyRows = vs.mapValues(_ => Seq[Any]())
    val seqRows = rdds.foldLeft(emptyRows) {
      case (seqs, (rdd, _)) =>
        seqs.sortedLeftOuterJoin(rdd).mapValues { case (seq, opt) => seq :+ opt.getOrElse(null) }
    }
    seqRows.values.map(sql.Row.fromSeq(_))
  }

  def schema = {
    val fields = attrs.toSeq.sortBy(_._1).map {
      case (name, (rdd, ttag)) =>
        sql.types.StructField(
          name = name,
          dataType = Table.dfType(ttag))
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

  // The output works the same way as in ImportDataFrame
  class Output(schema: sql.types.StructType)(implicit instance: MetaGraphOperationInstance)
    extends ImportDataFrame.Output(schema)

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
    extends TypedMetaGraphOp[ExecuteSQL.Input, ExecuteSQL.Output] {

  override lazy val hashCode = gUID.hashCode

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

  def outputMeta(instance: MetaGraphOperationInstance) = new Output(outputSchema)(instance)
  override def toJson = Json.obj(
    "sqlQuery" -> sqlQuery,
    "inputTables" -> inputTables,
    "outputSchema" -> outputSchema.prettyJson)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val sc = rc.sparkContext

    val entities = o.columns.values.map(_.entity)
    val entitiesByName = entities.map(e => (e.name, e): (Symbol, Attribute[_])).toMap
    val inOrder = outputSchema.map(f => entitiesByName(toSymbol(f)))

    val sqlContext = rc.sqlContext.newSession()

    for ((tableName, tableVs) <- inputs.tables) {
      val tc = inputs
        .tableColumns(tableName)
        .map {
          case (columnName, columnAttr) =>
            (columnName, (columnAttr.rdd, columnAttr.data.typeTag))
        }
        .toMap

      val rt = new AttributeTableRelation(tableVs.rdd, tc, sqlContext)
      val df = rt.toDF
      df.registerTempTable(tableName)
    }

    var df = sqlContext.sql(sqlQuery)
    rc.ioContext.writeAttributes(inOrder, ImportDataFrame.toNumberedLines(df, rc))
  }
}
