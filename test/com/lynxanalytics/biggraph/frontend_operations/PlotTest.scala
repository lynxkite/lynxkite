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
    "url": "/ajax/getTableOutput?q=%7B%22id%22:%224e6b8368-cc4f-303e-b5cf-0820990cc4c3%22,%22sampleRows%22:10%7D"
  }
  }"""))
  }
}
