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
    val path = parameters.getOrElse("path", "")
    val checkpoint = parameters.getOrElse("checkpoint", "")
    assert(
      path.isEmpty || checkpoint.isEmpty,
      s"You cannot define both a path ($path) and a checkpoint ($checkpoint).")
    val project = {
      implicit val metaManager = env.metaGraphManager
      if (path.nonEmpty) {
        controllers.DirectoryEntry.fromName(path).asProjectFrame.viewer
      } else {
        val cp = env.metaGraphManager.checkpointRepo.readCheckpoint(checkpoint)
        new controllers.RootProjectViewer(cp)
      }
    }
    new ProjectRelation(env, sqlContext, project)
  }
}

// TODO: Only vertex attributes are exposed at the moment.
class ProjectRelation(
  env: BigGraphEnvironment,
  val sqlContext: sql.SQLContext,
  project: controllers.ProjectViewer)
    extends sql.sources.BaseRelation with sql.sources.TableScan with sql.sources.PrunedScan {

  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager

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
