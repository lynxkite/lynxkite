package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.ml.regression.LinearRegressionModel
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.model._

class RegressionModelTrainerTest extends ModelTestBase {
  test("test model parameters") {
    val m = model(
      method = "Linear regression",
      labelName = "age",
      label = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      featureNames = List("yob"),
      attrs = Seq(Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955)),
      graph(4)).value

    assert(m.method == "Linear regression")
    assert(m.labelName == Some("age"))
    assert(m.featureNames == List("yob"))
    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(LinearRegressionModel.load(path).coefficients.size == 1)
  }
}
