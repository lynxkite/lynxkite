package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import play.api.libs.json

class PlotTest extends OperationsTestBase {
  test("Plot") {
    val plot = box("Create example graph")
      .box("Custom plot")
      .output("plot").plot
    assert(plot == json.Json.parse("""{
  "mark" : "bar",
  "encoding" : {
    "x" : {
      "field" : "name",
      "type" : "nominal"
    },
    "y" : {
      "field" : "age",
      "type" : "quantitative"
    }
  },
  "data": {
    "url": "downloadCSV?q=%7B%22id%22:%221df989fa-d79d-31c2-8baa-69a9553266fb%22,%22sampleRows%22:10000%7D",
    "format": {"type": "csv"}
  }
}"""))
  }
}
