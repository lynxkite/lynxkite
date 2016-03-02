package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_api._

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.table.TableRelation
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.controllers

import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.sql

import scala.collection.mutable;
import scala.reflect.runtime.universe._
import scala.reflect.{ classTag, ClassTag }

import java.util.UUID

// Provides methods for manipulating Spark DataFrames but it promises that it won't
// fire off any real Spark jobs.
class SQLHelper(
    sparkContext: spark.SparkContext,
    metaManager: MetaGraphManager,
    dataManager: DataManager) {

  // Creates a DataFrame based on a query, assuming that tables
  // are sub-tables of a top-level project.
  def getDataFrame(project: controllers.ProjectViewer, sql: String): spark.sql.DataFrame =
    metaManager.synchronized {
      implicit val mm = metaManager
      implicit val dm = dataManager
      val sqlContext = dataManager.newSQLContext()
      for (path <- project.allRelativeTablePaths) {
        val tableName = path.toString
        val table = controllers.Table(path, project)
        val dataFrame = (new TableRelation(table, sqlContext)).toDF
        dataFrame.registerTempTable(tableName)
      }
      sqlContext.sql(sql)
    }

  // A fake relation that gives back empty RDDs and records all
  // the columns needed for the computation.
  private[spark_util] class InputGUIDCollectingFakeTableRelation(
    table: controllers.Table,
    sqlContext: sql.SQLContext,
    sparkContext: spark.SparkContext,
    columnListSaver: Seq[(UUID, String)] => Unit)
      extends TableRelation(table, sqlContext)(null) {

    // TableScan
    override def buildScan(): rdd.RDD[sql.Row] = buildScan(schema.fieldNames)

    // PrunedScan
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
  def getInputColumns(project: controllers.ProjectViewer, sqlQuery: String): Map[String, Seq[(UUID, String)]] =
    metaManager.synchronized {
      // This implementation exploits that DataFrame.explain()
      // scans all the input columns. We create fake input table
      // relations below, and collect the scanned columns in
      // `columnAccumulator`.
      val columnAccumulator = mutable.Map[String, Seq[(UUID, String)]]()
      implicit val mm = metaManager
      //implicit val dm = dataManager
      val sqlContext = dataManager.newSQLContext()
      for (path <- project.allRelativeTablePaths) {
        val tableName = path.toString
        val dataFrame = (
          new InputGUIDCollectingFakeTableRelation(
            controllers.Table(path, project),
            sqlContext,
            sparkContext,
            columnList => { columnAccumulator(tableName) = columnList }
          )).toDF
        dataFrame.registerTempTable(tableName)
      }
      val df = sqlContext.sql(sqlQuery)
      sql.SQLHelperHelper.explainQuery(df)
      columnAccumulator.toMap
    }

}
