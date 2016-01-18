// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class SQLRequest(project: String, sql: String, maxRows: Int)
case class SQLResult(header: List[String], data: List[List[String]])

case class SQLExportRequest(
  project: String,
  sql: String,
  format: String,
  path: String,
  options: Map[String, String])

class SQLController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager

  private def projectSQL(user: serving.User, project: String, sql: String): DataFrame = {
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

  def runSQLQuery(user: serving.User, request: SQLExportRequest): Unit = {
    val df = projectSQL(user, request.project, request.sql)

    SQLResult(
      header = result.columns.toList,
      data = df.head(request.maxRows).map {
        row =>
          row.toSeq.map {
            case null => "null"
            case item => item.toString
          }.toList
      }.toList
    )
  }

  def exportSQLQuery(user: serving.User, request: SQLExportRequest): Unit = {
    val df = projectSQL(user, request.project, request.sql)
    val path = if (request.path == "<download>") {
        dataManager.repositoryPath / "exports" / graph_util.Timestamp.toString
      } else {
        HadoopFile(request.path)
      }
    // Write out the data.
    df.write.format(request.format).options(request.options).save(path.resolvedName)
    // Redirect to the download link if necessary.
    if (request.path == "<download>") {
      val urlPath = java.net.URLEncoder.encode(path.symbolicName, "utf-8")                              
      val urlName = java.net.URLEncoder.encode(path.path.getName, "utf-8")                                         
      val url = s"/download?path=$urlPath&name=$urlName"   
      throw new serving.FlyingResult(play.api.mvc.Results.Redirect(url))
    }
  }
}
