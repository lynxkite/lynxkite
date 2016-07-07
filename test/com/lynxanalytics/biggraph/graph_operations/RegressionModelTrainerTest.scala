package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.model._

class RegressionModelTrainerTest extends ModelTestBase {
  def checkModel(method: String) {
    val m = model(
      method = method,
      labelName = "age",
      label = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      featureNames = List("yob"),
      attrs = Seq(Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955)),
      graph(4)).value

    assert(m.method == method)
    assert(m.labelName == Some("age"))
    assert(m.featureNames == List("yob"))
    val impl = m.load(sparkContext)
    val yob = vectorsRDD(Array(2000))
    val age = m.scaleBack(impl.transform(
      yob.map(v => m.featureScaler.transform(v)))).collect()(0)
    assertRoughlyEquals(age, 15, 1)
  }

  test("test model parameters") {
    checkModel("Linear regression")
    checkModel("Ridge regression")
    checkModel("Lasso")
  }
}
