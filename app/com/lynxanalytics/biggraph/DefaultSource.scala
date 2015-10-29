// A DataFrame datasource for LynxKite projects.
package com.lynxanalytics.biggraph

import org.apache.spark.rdd
import org.apache.spark.sql

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

object DefaultSource {
  var env: BigGraphEnvironment = null // Someone set this please.
}
class DefaultSource extends sql.sources.RelationProvider {
  def createRelation(sqlContext: sql.SQLContext, parameters: Map[String, String]) =
    new ProjectRelation(DefaultSource.env, sqlContext, parameters("path"))
}

class ProjectRelation(env: BigGraphEnvironment, val sqlContext: sql.SQLContext, projectName: String)
    extends sql.sources.BaseRelation with sql.sources.TableScan with sql.sources.PrunedScan {

  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager
  val project = controllers.ProjectFrame.fromName(projectName).viewer.editor

  // BaseRelation
  val schema: sql.types.StructType = {
    val fields = project.vertexAttributes.toSeq.map {
      case (name, attr) =>
        sql.types.StructField(
          name = name,
          dataType = sql.catalyst.ScalaReflection.schemaFor(attr.typeTag).dataType)
      // TODO: Use ScalaReflection.dataTypeFor when it's released.
    }
    sql.types.StructType(fields)
  }

  // TableScan
  def buildScan(): rdd.RDD[sql.Row] = buildScan(project.vertexAttributeNames.toArray)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]): rdd.RDD[sql.Row] = {
    val rdds = requiredColumns.toSeq.map(name => project.vertexAttributes(name).rdd)
    val emptyRows = project.vertexSet.rdd.mapValues(_ => Seq[Any]())
    val seqRows = rdds.foldLeft(emptyRows) { (seqs, rdd) =>
      seqs.sortedLeftOuterJoin(rdd).mapValues { case (seq, opt) => seq :+ opt.getOrElse(null) }
    }
    seqRows.values.map(sql.Row.fromSeq(_))
  }
}
