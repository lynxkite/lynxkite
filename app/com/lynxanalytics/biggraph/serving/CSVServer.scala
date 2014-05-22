package com.lynxanalytics.biggraph.serving

import java.util.UUID
import play.api.mvc

import com.lynxanalytics.biggraph.BigGraphProductionEnviroment._
import com.lynxanalytics.biggraph.graph_api.GraphData
import com.lynxanalytics.biggraph.graph_util.CSVExport

object CSVServer extends mvc.Controller {
  def verticesCSVGet = dataGet { data =>
      Ok(CSVExport.exportVertices(data).toString)
        .as("text/csv")
        .withHeaders("Content-disposition" -> "attachment; filename=vertices-%s.csv"
                       .format(data.bigGraph.gUID));
    }

  def edgesCSVGet = dataGet { data =>
      Ok(CSVExport.exportEdges(data).toString)
        .as("text/csv")
        .withHeaders("Content-disposition" -> "attachment; filename=edges-%s.csv"
                       .format(data.bigGraph.gUID));
    }

  private def dataGet(responseRenderer: GraphData => mvc.Result) =
    mvc.Action { request =>
      val guid = UUID.fromString(request.getQueryString("guid").get)
      val graph = bigGraphManager.graphForGUID(guid).get
      val data = graphDataManager.obtainData(graph)
      responseRenderer(data)
    }
}
