package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

case class SaveGraphRequest(id: String)

class PersistenceController(enviroment: BigGraphEnvironment) {
  def saveGraph(request: SaveGraphRequest): serving.Empty = {
    val graph = BigGraphController.getBigGraphForId(request.id, enviroment)
    enviroment.graphDataManager.saveDataToDisk(graph)
    serving.Empty()
  }
}

