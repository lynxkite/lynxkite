// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class SQLRequest(project: String, sql: String)
case class SQLResult(header: Array[String], data: Array[Seq[String]])

class SQLController(val env: BigGraphEnvironment) {
  private val ReportedRowNum = 10
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager

  def runQuery(user: serving.User, request: SQLRequest): SQLResult = metaManager.synchronized {
    val p = SubProject.parsePath(request.project)
    assert(p.frame.exists, s"Project ${request.project} does not exist.")
    p.frame.assertReadAllowedFrom(user)

    // Every query runs in its own SQLContext for isolation.
    implicit val sqlContext = dataManager.newSQLContext()
    val v = p.viewer
    v.allRelativeTablePaths.foreach {
      tableName => Table.fromCanonicalPath(tableName, v).toDF.registerTempTable(tableName)
    }
    log.info(s"Trying to execute query: ${request.sql}")
    val result = sqlContext.sql(request.sql)

    SQLResult(
      header = result.columns,
      data = result.head(ReportedRowNum).map { row => row.toSeq.map { item => item.toString } }
    )
  }
}
