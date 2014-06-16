package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_util
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class SaveGraphAsCSVRequest(
  id: String,
  targetDirPath: String,
  awsAccessKeyId: String,
  awsSecretAccessKey: String)

case class SaveGraphAsCSVResponse(
  success: Boolean = true,
  failureReason: String = "")
/*
class GraphExportController(enviroment: BigGraphEnvironment) {
  def saveGraphAsCSV(request: SaveGraphAsCSVRequest): SaveGraphAsCSVResponse = {
    val graph = BigGraphController.getBigGraphForId(request.id, enviroment)
    val data = enviroment.graphDataManager.obtainData(graph)
    Try(graph_util.CSVExport.exportToDirectory(data, Filename(request.targetDirPath, request.awsAccessKeyId, request.awsSecretAccessKey))) match {
      case Success(_) => SaveGraphAsCSVResponse()
      case Failure(e) => SaveGraphAsCSVResponse(false, e.toString)
    }
  }
}

 */
