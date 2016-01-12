// A DataFrame datasource for LynxKite projects.
package com.lynxanalytics.biggraph.table

import org.apache.spark.rdd
import org.apache.spark.sql

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util

object DefaultSource {
  // Returns a unique ID which has to be passed as the "environment" parameter of the
  // DataFrameReader. Use BigGraphEnvironment.dataFrame for the most convenient access.
  def register(env: BigGraphEnvironment): String = {
    val id = s"env-${graph_util.Timestamp}"
    envs(id) = env
    id
  }
  private val envs = collection.mutable.Map[String, BigGraphEnvironment]()
}
class DefaultSource extends sql.sources.RelationProvider {
  def createRelation(sqlContext: sql.SQLContext, parameters: Map[String, String]) = {
    val env = DefaultSource.envs(parameters("environment"))
    val path = parameters("path")
    val table = controllers.Table.fromGlobalPath(path)(env.metaGraphManager)
    new TableRelation(table)(env.dataManager)
  }
}

class TableRelation(
  table: controllers.Table)(implicit dataManager: DataManager)
    extends sql.sources.BaseRelation with sql.sources.TableScan with sql.sources.PrunedScan {

  def toDF = sqlContext.baseRelationToDataFrame(this)

  // BaseRelation
  val sqlContext = dataManager.sqlContext
  val schema = table.dataFrameSchema

  // TableScan
  def buildScan(): rdd.RDD[sql.Row] = buildScan(schema.fieldNames)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]): rdd.RDD[sql.Row] = {
    val rdds = requiredColumns.toSeq.map(name => table.columns(name).rdd)
    val emptyRows = table.idSet.rdd.mapValues(_ => Seq[Any]())
    val seqRows = rdds.foldLeft(emptyRows) { (seqs, rdd) =>
      seqs.sortedLeftOuterJoin(rdd).mapValues { case (seq, opt) => seq :+ opt.getOrElse(null) }
    }
    seqRows.values.map(sql.Row.fromSeq(_))
  }
}
