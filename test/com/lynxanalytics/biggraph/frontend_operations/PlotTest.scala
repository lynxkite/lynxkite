package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class PlotTest extends OperationsTestBase {
  test("Plot") {
    val plot = box("Create example graph")
      .box("Custom plot")
      .output("plot").plot.value
    assert(plot == """{
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
  "data" : {
    "values" : [
      {
        "name" : "Adam",
        "location" : "(40.71448,-74.00598)",
        "age" : 20.3,
        "id" : 0,
        "income" : 1000.0,
        "gender" : "Male"
      },
      {
        "name" : "Eve",
        "location" : "(47.5269674,19.0323968)",
        "age" : 18.2,
        "id" : 1,
        "gender" : "Female"
      },
      {
        "name" : "Bob",
        "location" : "(1.352083,103.819836)",
        "age" : 50.3,
        "id" : 2,
        "income" : 2000.0,
        "gender" : "Male"
      },
      {
        "name" : "Isolated Joe",
        "location" : "(-33.8674869,151.2069902)",
        "age" : 2.0,
        "id" : 3,
        "gender" : "Male"
      }
    ]
  }
}""")
  }
}
