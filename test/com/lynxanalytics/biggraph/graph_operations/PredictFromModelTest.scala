package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class PredictFromModelTest extends ModelTestBase {
  test("test prediction from model") {
    val g = graph(4)
    val m = model(
      method = "Linear regression",
      labelName = "age",
      label = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      featureNames = List("yob"),
      attrs = Seq(Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955)),
      graph = g)

    val yob = Seq(AddVertexAttribute.run(g.vs, Map(0 -> 2000.0)))
    val age = predict(m, yob).rdd.values.collect()(0)
    assertRoughlyEquals(age, 15, 1)
  }
}
