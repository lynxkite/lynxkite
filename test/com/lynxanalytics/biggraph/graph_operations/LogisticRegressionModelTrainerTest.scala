package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.ml.classification.LogisticRegressionModel
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class LogisticRegressionModelTrainerTest extends ModelTestBase {

  test("example graph by age") {
    val m = model(
      method = "Logistic regression",
      labelName = "isSenior",
      label = Map(0 -> 0, 1 -> 1, 2 -> 0, 3 -> 1),
      featureNames = List("age"),
      attrs = Seq(Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60)),
      graph(4)).value

    assert(m.method == "Logistic regression")
    assert(m.featureNames == List("age"))
    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(LogisticRegressionModel.load(path).coefficients.size == 1)
  }
}
