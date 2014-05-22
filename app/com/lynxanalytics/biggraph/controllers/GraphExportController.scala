package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnviroment
import com.lynxanalytics.biggraph.graph_util
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class SaveGraphAsCSVRequest(
  id: String,
  targetDirPath: String)

case class SaveGraphAsCSVResponse(
  success: Boolean = true,
  failureReason: String = "")

class GraphExportController(enviroment: BigGraphEnviroment) {
  def saveGraphAsCSV(request: SaveGraphAsCSVRequest): SaveGraphAsCSVResponse = {
    val graph = BigGraphController.getBigGraphForId(request.id, enviroment)
    val data = enviroment.graphDataManager.obtainData(graph)
    Try(graph_util.CSVExport.exportToDirectory(data, request.targetDirPath)) match {
      case Success(_) => SaveGraphAsCSVResponse()
      case Failure(e) => SaveGraphAsCSVResponse(false, e.toString)
    }
  }
}

