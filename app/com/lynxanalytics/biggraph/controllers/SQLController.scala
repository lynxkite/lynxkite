// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import org.apache.spark

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class SQLQueryRequest(project: String, sql: String, maxRows: Int)
case class SQLQueryResult(header: List[String], data: List[List[String]])

case class SQLExportRequest(
  project: String,
  sql: String,
  format: String,
  path: String,
  options: Map[String, String])
case class SQLExportResult(download: Option[String])

class SQLController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager

  private def projectSQL(user: serving.User, project: String, sql: String): spark.sql.DataFrame = {
    val tables = metaManager.synchronized {
      val p = SubProject.parsePath(project)
      assert(p.frame.exists, s"Project ${project} does not exist.")
      p.frame.assertReadAllowedFrom(user)

      val v = p.viewer
      v.allRelativeTablePaths.map {
        tableName => (tableName -> Table.fromCanonicalPath(tableName, v))
      }
    }
    // Every query runs in its own SQLContext for isolation.
    val sqlContext = dataManager.newSQLContext()
    for ((tableName, table) <- tables) {
      table.toDF(sqlContext).registerTempTable(tableName)
    }

    log.info(s"Trying to execute query: ${sql}")
    sqlContext.sql(sql)
  }

  def runSQLQuery(user: serving.User, request: SQLQueryRequest): SQLQueryResult = {
    val df = projectSQL(user, request.project, request.sql)

    SQLQueryResult(
      header = df.columns.toList,
      data = df.head(request.maxRows).map {
        row =>
          row.toSeq.map {
            case null => "null"
            case item => item.toString
          }.toList
      }.toList
    )
  }

  def exportSQLQuery(user: serving.User, request: SQLExportRequest): SQLExportResult = {
    val df = projectSQL(user, request.project, request.sql)
    val path = if (request.path == "<download>") {
      dataManager.repositoryPath / "exports" / graph_util.Timestamp.toString + "." + request.format
    } else {
      graph_util.HadoopFile(request.path)
    }
    val format = request.format match {
      case "csv" => "com.databricks.spark.csv"
      case x => x
    }
    df.write.format(format).options(request.options).save(path.resolvedName)
    SQLExportResult(
      download = if (request.path == "<download>") Some(path.symbolicName) else None)
  }
}
