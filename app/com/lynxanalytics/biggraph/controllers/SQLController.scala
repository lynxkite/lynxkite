// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class SQLRequest(project: String, sql: String, rownum: Int)
case class SQLResult(header: List[String], data: List[List[String]])

class SQLController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager

  def runSQLQuery(user: serving.User, request: SQLRequest): SQLResult = {
    val tables = metaManager.synchronized {
      val p = SubProject.parsePath(request.project)
      assert(p.frame.exists, s"Project ${request.project} does not exist.")
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

    log.info(s"Trying to execute query: ${request.sql}")
    val result = sqlContext.sql(request.sql)

    SQLResult(
      header = result.columns.toList,
      data = result.head(request.rownum).map {
        row =>
          row.toSeq.map {
            case null => "null"
            case item => item.toString
          }.toList
      }.toList
    )
  }
}
