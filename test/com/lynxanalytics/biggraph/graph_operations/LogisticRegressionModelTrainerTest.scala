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

  // This test proved to be unreliable. On some machines it passed on others it failed. We don't know the reason.
  // https://github.com/biggraph/biggraph/issues/5501
  ignore("z-scores") {
    import sparkSession.implicits._
    import org.scalactic.Tolerance._
    import ml.linalg.Vectors.{ dense => mlVector }
    val coefficientsAndIntercept = Array(1.0, -1345763.0, -0.01)
    val predictions = sparkSession.createDataset(Seq((mlVector(3000000.0, 1.0), mlVector(0.88, 0.12), 1.0),
      (mlVector(1.0, 278.01), mlVector(0.26, 0.74), 0.0)
    )).toDF("features", "probability", "label")
    val zValues = LogisticRegressionModelTrainer.computeZValues(coefficientsAndIntercept, predictions)
    val expected = Array(0.01586, -1.97108, 0.0)
    for ((e, r) <- expected.zip(zValues)) {
      assert(e === r +- 0.0001)
    }
  }

  // to see, how data was obtained, check https://github.com/biggraph/biggraph/issues/5501
  test("z-score for real model") {
    import sparkSession.implicits._
    import org.scalactic.Tolerance._
    import ml.linalg.Vectors.{ dense => mlVector }
    val coefficientsAndIntercept = Array(-0.052874, -0.891587, 8.004224)
    val X = Seq(
      mlVector(0.0, 9.0),
      mlVector(0.0, 8.0),
      mlVector(1.0, 7.0),
      mlVector(1.0, 5.0),
      mlVector(2.0, 1.0),
      mlVector(3.0, 1.0),
      mlVector(1.0, 10.0),
      mlVector(2.0, 10.0),
      mlVector(3.0, 7.0),
      mlVector(8.0, 8.0)
    )
    val probabilities = Seq(0.49498463, 0.70506297, 0.84686725, 0.97049978, 0.9990952, 0.99904612, 0.27597346,
      0.2655347, 0.83264514, 0.61029074).map(p => mlVector(p, 1 - p))
    val labels = Seq(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0)
    val predictions = sparkSession.createDataset(X zip probabilities zip labels map { case ((x, p), l) => (x, p, l) })
      .toDF("features", "probability", "label")
    val zValues = LogisticRegressionModelTrainer.computeZValues(coefficientsAndIntercept, predictions)
    val expected = Array(-0.170393, -1.271952, 1.294992)
    for ((e, r) <- expected.zip(zValues)) {
      assert(e === r +- 0.000001)
    }
  }
}
