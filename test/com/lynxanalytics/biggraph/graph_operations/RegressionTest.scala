package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class RegressionTest extends FunSuite with TestGraphOp {
  def predict(method: String, label: Attribute[Double], features: Seq[Attribute[Double]]) = {
    val op = Regression(method, 1)
    val prediction = op(op.features, features)(op.label, label).result.prediction
    prediction.rdd.collect.toMap
  }

  def assertRoughly(
    result: Map[Long, Double], expectation: Map[Long, Double], maxError: Double) = {
    assert(result.size == expectation.size)
    val error = result.map { case (k, v) => Math.abs(v - expectation(k)) }
    assert(error.max <= maxError, s"$result is unlike $expectation")
  }

  def predictLabelFromAttr(
    method: String,
    label: Map[Int, Double],
    attr: Map[Int, Double]): Map[Long, Double] = {
    // Create the graph from attr in case of missing labels.
    val g = SmallTestGraph(attr.mapValues(_ => Seq())).result
    val labelOp = AddDoubleVertexAttribute(label)
    val l = labelOp(labelOp.vs, g.vs).result.attr
    val attrOp = AddDoubleVertexAttribute(attr)
    val a = attrOp(attrOp.vs, g.vs).result.attr
    predict(method, l, Seq(a))
  }

  def testRegressions(
    label: Map[Int, Double],
    attr: Map[Int, Double],
    expectation: Map[Long, Double],
    maxError: Double) {
    for (method <- Seq("Linear regression", "Ridge regression", "Lasso")) {
      println("       . " + method)
      assertRoughly(
        predictLabelFromAttr(method, label, attr),
        expectation,
        maxError)
    }
  }

  // More like regression.
  test("income from age") {
    def incomes(method: String) = {
      println("       . " + method)
      val g = ExampleGraph()().result
      predict(method, g.income, Seq(g.age))
    }
    assertRoughly(incomes("Linear regression"),
      Map(0L -> 1000.0, 1L -> 930.0, 2L -> 2000.0, 3L -> 400.0), maxError = 10)
    assertRoughly(incomes("Ridge regression"),
      Map(0L -> 1010.0, 1L -> 945.0, 2L -> 1985.0, 3L -> 420.0), maxError = 10)
    assertRoughly(incomes("Lasso"),
      Map(0L -> 1010.0, 1L -> 945.0, 2L -> 1985.0, 3L -> 420.0), maxError = 10)
    assertRoughly(incomes("Naive Bayes"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 1000.0, 3L -> 1000.0), maxError = 10)
    assertRoughly(incomes("Decision tree"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 2000.0, 3L -> 1000.0), maxError = 10)
    assertRoughly(incomes("Random forest"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 1300.0, 3L -> 1000.0), maxError = 10)
    assertRoughly(incomes("Gradient-boosted trees"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 2000.0, 3L -> 1000.0), maxError = 10)
  }

  // More like classification.
  test("gender from age") {
    def gender(method: String) = {
      println("       . " + method)
      val g = ExampleGraph()().result
      val gender = DeriveJS.deriveFromAttributes[Double](
        "gender == 'Male' ? 1 : 0", Seq("gender" -> g.gender), g.vertices).attr
      predict(method, gender, Seq(g.age))
    }
    assertRoughly(gender("Linear regression"),
      Map(0L -> 0.7, 1L -> 0.7, 2L -> 0.8, 3L -> 0.7), maxError = 0.1)
    assertRoughly(gender("Ridge regression"),
      Map(0L -> 0.7, 1L -> 0.7, 2L -> 0.8, 3L -> 0.7), maxError = 0.1)
    assertRoughly(gender("Lasso"),
      Map(0L -> 0.7, 1L -> 0.7, 2L -> 0.8, 3L -> 0.7), maxError = 0.1)
    assertRoughly(gender("Logistic regression"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.1)
    assertRoughly(gender("Naive Bayes"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.1)
    assertRoughly(gender("Decision tree"),
      Map(0L -> 1.0, 1L -> 0.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.1)
    assertRoughly(gender("Random forest"),
      Map(0L -> 1.0, 1L -> 0.5, 2L -> 1.0, 3L -> 0.8), maxError = 0.1)
    assertRoughly(gender("Gradient-boosted trees"),
      Map(0L -> 1.0, 1L -> 0.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.1)
  }

  test("regression - age from year of birth") {
    testRegressions(
      label = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      attr = Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955),
      expectation = Map(0L -> 25, 1L -> 40, 2L -> 30, 3L -> 60),
      maxError = 1)
  }

  test("regression - year of birth from age") {
    testRegressions(
      label = Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955),
      attr = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      expectation = Map(0L -> 1990, 1L -> 1975, 2L -> 1985, 3L -> 1955),
      maxError = 1)
  }

  test("regression - corner cases - predict constant") {
    testRegressions(
      label = Map(0 -> 5, 1 -> 5, 2 -> 5),
      attr = Map(0 -> 10, 1 -> 50, 2 -> 100),
      expectation = Map(0L -> 5, 1L -> 5, 2L -> 5),
      maxError = 0.1)
  }

  test("regression - corner cases - predict from constant") {
    // Should not throw an error.
    testRegressions(
      label = Map(0 -> 1, 1 -> 2, 2 -> 3),
      attr = Map(0 -> 5, 1 -> 5, 2 -> 5),
      expectation = Map(0L -> 2, 1L -> 2, 2L -> 2),
      maxError = 1)
  }

  test("regression - corner cases - noisy data") {
    // y ~= 100 - 10x.
    testRegressions(
      label = Map(0 -> 91, 1 -> 79, 2 -> 72),
      attr = Map(0 -> 1, 1 -> 2, 2 -> 3),
      expectation = Map(0L -> 90, 1L -> 80, 2L -> 70),
      maxError = 5)
  }

  test("regression - corner cases - missing labels") {
    testRegressions(
      label = Map(0 -> 10, 1 -> 20, 2 -> 30),
      // Significantly different means and stddev with and without the extra labels.
      attr = Map(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 10, 4 -> 20),
      expectation = Map(0L -> 10, 1L -> 20, 2L -> 30, 3L -> 100, 4L -> 200),
      maxError = 10)
  }
}
