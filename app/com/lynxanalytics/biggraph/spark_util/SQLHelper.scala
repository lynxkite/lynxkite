package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_api._

import com.lynxanalytics.biggraph.table.BaseRelation
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.protection.Limitations

import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types

import scala.collection.mutable;
import scala.reflect.runtime.universe._

import java.util.UUID

// Provides methods for manipulating Spark DataFrames but it promises that it won't
// fire off any real Spark jobs.
class SQLHelper(
    sparkContext: spark.SparkContext,
    metaManager: MetaGraphManager,
    dataManager: DataManager) {

  // A fake relation that gives back empty RDDs and records all
  // the columns needed for the computation.
  private[spark_util] class InputGUIDCollectingFakeTableRelation(
    table: controllers.Table,
    sqlContext: sql.SQLContext,
    sparkContext: spark.SparkContext,
    columnListSaver: Seq[(UUID, String)] => Unit)
      extends BaseRelation(table, sqlContext) {
    override def buildScan(requiredColumns: Array[String]): rdd.RDD[sql.Row] = {
      val guids = requiredColumns
        .toSeq
        .map {
          columnName =>
            (
              table.columns(columnName).gUID,
              columnName
            )
        }
      columnListSaver(guids)
      sparkContext.emptyRDD
    }
  }

  // Given a project and a query, collects the the guids of the
  // input attributes required to execute the query.
  // The result is a map of tableName -> Seq(guid, columnName)
  def getInputColumns(project: controllers.ProjectViewer,
                      sqlQuery: String): (Map[String, Seq[(UUID, String)]], DataFrame) = {
    // This implementation exploits that DataFrame.explain()
    // scans all the input columns. We create fake input table
    // relations below, and collect the scanned columns in
    // `columnAccumulator`.
    val columnAccumulator = mutable.Map[String, Seq[(UUID, String)]]()
    val sqlContext = dataManager.newSQLContext()
    val dfs = project.allRelativeTablePaths.map { path =>
      val tableName = path.toString
      val dataFrame = (
        new InputGUIDCollectingFakeTableRelation(
          controllers.Table(path, project),
          sqlContext,
          sparkContext,
          columnList => { columnAccumulator(tableName) = columnList }
        )).toDF
      tableName -> dataFrame
    }
    val df = DataManager.sql(sqlContext, sqlQuery, dfs.toList)
    sql.SQLHelperHelper.explainQuery(df)
    (columnAccumulator.toMap, df)
  }

  def sqlToTable(project: controllers.ProjectViewer, sqlQuery: String): controllers.Table = {
    implicit val m = metaManager
    val (inputTables, dataFrame) = getInputColumns(project, sqlQuery)
    val op = new graph_operations.ExecuteSQL(
      sqlQuery,
      inputTables.map {
        case (tableName, columnList) =>
          (tableName, columnList.map(_._2))
      },
      dataFrame.schema)
    var opBuilder = op()
    for ((tableName, columns) <- inputTables) {
      for ((guid, columnName) <- columns) {
        val attrKey = op.tableColumns(tableName)(columnName)
        val attrEntity = metaManager
          .attribute(guid)
        opBuilder = opBuilder(attrKey, attrEntity)
      }
    }
    val res = opBuilder.result
    controllers.RawTable(res.ids, res.columns.mapValues(_.entity))
  }

}
object SQLHelper {
  private def toSymbol(field: types.StructField) = Symbol("imported_column_" + field.name)

  private def isTuple2Type(st: types.StructType) =
    st.size == 2 && st(0).name == "_1" && st(1).name == "_2"

  // I really don't understand why this isn't part of the spark API, but I can't find it.
  // So here it goes.
  def typeTagFromDataType(dataType: types.DataType): TypeTag[_] = {
    import scala.reflect.runtime.universe._
    dataType match {
      case at: types.ArrayType => TypeTagUtil.arrayTypeTag(typeTagFromDataType(at.elementType))
      case _: types.BinaryType => typeTag[Array[Byte]]
      case _: types.BooleanType => typeTag[Boolean]
      case _: types.ByteType => typeTag[Byte]
      case _: types.DateType => typeTag[java.sql.Date]
      case _: types.DecimalType => typeTag[java.math.BigDecimal]
      case _: types.DoubleType => typeTag[Double]
      case _: types.FloatType => typeTag[Float]
      case _: types.IntegerType => typeTag[Int]
      case _: types.LongType => typeTag[Long]
      case mt: types.MapType =>
        TypeTagUtil.mapTypeTag(typeTagFromDataType(mt.keyType), typeTagFromDataType(mt.valueType))
      case _: types.ShortType => typeTag[Short]
      case _: types.StringType => typeTag[String]
      case _: types.TimestampType => typeTag[java.util.Date]
      case st: types.StructType if isTuple2Type(st) =>
        TypeTagUtil.tuple2TypeTag(
          typeTagFromDataType(st(0).dataType),
          typeTagFromDataType(st(1).dataType))
      case x => throw new AssertionError(s"Unsupported type in DataFrame: $x")
    }
  }

  private def processDataFrameRow(tupleColumnIdList: Seq[Int])(row: Row): Seq[Any] = {
    var result = row.toSeq
    // A simple row.toSeq would be enough for this method, except
    // that tuple-typed columns need special handling.
    for (columnId <- tupleColumnIdList) {
      val column = result(columnId).asInstanceOf[Row]
      result = result.updated(columnId, (column(0), column(1)))
    }
    result
  }

  // Collects positions of columns which contain tuples.
  private def getTupleColumnIdList(schema: types.StructType): Seq[Int] = {
    schema
      .map(field => field.dataType)
      .zipWithIndex
      // Only keep the index of tuple items:
      .collect {
        case (st: types.StructType, id) if isTuple2Type(st) =>
          id
      }
  }

  def toSeqRDD(dataFrame: DataFrame): rdd.RDD[Seq[Any]] = {
    val tupleColumnIdList = getTupleColumnIdList(dataFrame.schema)
    dataFrame.rdd.map(processDataFrameRow(tupleColumnIdList))
  }

  private def toNumberedLines(dataFrame: DataFrame, rc: RuntimeContext): AttributeRDD[Seq[Any]] = {
    val numRows = dataFrame.count()
    val maxRows = Limitations.maxImportedLines
    if (maxRows >= 0) {
      if (numRows > maxRows) {
        throw new AssertionError(
          s"Can't import $numRows lines as your licence only allows $maxRows.")
      }
    }
    val seqRDD = toSeqRDD(dataFrame)
    val partitioner = rc.partitionerForNRows(numRows)
    import com.lynxanalytics.biggraph.spark_util.Implicits._
    seqRDD.randomNumbered(partitioner)
  }

  // Magic output for metagraph operations whose output is created
  // from a DataFrame.
  class DataFrameOutput(schema: types.StructType)(implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    // Methods for listing the output entities for metagraph building purposes.
    private def attributeFromTypeTag[T: TypeTag](
      ids: => EntityContainer[VertexSet], name: scala.Symbol): EntityContainer[Attribute[T]] =
      vertexAttribute[T](ids, name)

    private def attributeFromField(
      ids: => EntityContainer[VertexSet],
      field: types.StructField): EntityContainer[Attribute[_]] = {
      attributeFromTypeTag(ids, toSymbol(field))(typeTagFromDataType(field.dataType))
    }

    val ids = vertexSet
    val columns = schema.map {
      field => field.name -> attributeFromField(ids, field)
    }.toMap

    // Methods for populating this output instance with computed output RDDs.
    def populateOutput(
      rc: RuntimeContext,
      schema: types.StructType,
      dataFrame: DataFrame) {
      val entities = this.columns.values.map(_.entity)
      val entitiesByName = entities.map(e => (e.name, e): (scala.Symbol, Attribute[_])).toMap
      val inOrder = schema.map(f => entitiesByName(SQLHelper.toSymbol(f)))

      rc.ioContext.writeAttributes(inOrder, toNumberedLines(dataFrame, rc))
    }
  }

}
