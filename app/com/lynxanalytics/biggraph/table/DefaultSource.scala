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
    val table = controllers.Table(controllers.GlobalTablePath.parse(path))(env.metaGraphManager)
    new TableRelation(table, sqlContext, env.dataManager)
  }
}

abstract class BaseRelation(
  table: controllers.Table, val sqlContext: sql.SQLContext)
    extends sql.sources.BaseRelation with sql.sources.TableScan with sql.sources.PrunedScan {
  def toDF = sqlContext.baseRelationToDataFrame(this)

  // BaseRelation
  val schema = table.dataFrameSchema

  // TableScan
  def buildScan(): rdd.RDD[sql.Row] = buildScan(schema.fieldNames)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]): rdd.RDD[sql.Row]
}

class RDDRelation(
  table: controllers.Table,
  sqlContext: sql.SQLContext,
  val vertexSetRDD: VertexSetRDD,
  val columnRDDs: Map[String, AttributeRDD[_]])
    extends BaseRelation(table, sqlContext) {
  def buildScan(requiredColumns: Array[String]): rdd.RDD[sql.Row] = {
    val rdds = requiredColumns.toSeq.map(name => columnRDDs(name))
    val emptyRows = vertexSetRDD.mapValues(_ => Seq[Any]())
    val seqRows = rdds.foldLeft(emptyRows) { (seqs, rdd) =>
      seqs.sortedLeftOuterJoin(rdd).mapValues { case (seq, opt) => seq :+ opt.getOrElse(null) }
    }
    seqRows.values.map(sql.Row.fromSeq(_))
  }
}

class TableRelation(
  table: controllers.Table, sqlContext: sql.SQLContext, dataManager: DataManager)
    extends RDDRelation(table, sqlContext, null, null) {
  // Get RDDs up-front. We do not want to access the DataManager later, when we are potentially
  // inside a running ImportDataFrame operation. (#5580)
  override val vertexSetRDD: VertexSetRDD = dataManager.get(table.idSet).rdd
  override val columnRDDs: Map[String, AttributeRDD[_]] =
    schema.fieldNames.toSeq.map(name => name -> table.columnForDF(name)(dataManager)).toMap
}
