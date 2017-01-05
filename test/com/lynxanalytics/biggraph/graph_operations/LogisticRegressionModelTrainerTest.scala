package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.ml.classification.LogisticRegressionModel
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_api.Scripting._
import org.apache.spark.ml

class LogisticRegressionModelTrainerTest extends ModelTestBase {

  test("small graph by age") {
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

  test("boundary condition when all labels equal to 1.0") {
    val m = model(
      method = "Logistic regression",
      labelName = "isSenior",
      label = Map(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 3 -> 1.0),
      featureNames = List("age"),
      attrs = Seq(Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60)),
      graph(4)).value

    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(LogisticRegressionModel.load(path).coefficients.size == 1)
  }

  test("z-scores") {
    import sparkSession.implicits._
    import org.scalactic.Tolerance._
    import ml.linalg.Vectors.{ dense => mlVector }
    val coefficientsAndIntercept = Array(1.0, -1345763.0, -0.01)
    val predictions = sparkSession.createDataset(Seq((mlVector(3000000.0, 1.0), mlVector(0.88, 0.12), 1.0),
      (mlVector(1.0, 278.01), mlVector(0.26, 0.74), 0.0)
    )).toDF("features", "probability", "label")
    val zValues = LogisticRegressionModelTrainer.computeZValues(coefficientsAndIntercept, predictions)
    val expected = Array(0.01586212412069695, -1.971080062166662, -5.2683560638617534E-11)
    for ((e, r) <- expected.zip(zValues)) {
      assert(e === r +- 0.0001)
    }
  }
}
