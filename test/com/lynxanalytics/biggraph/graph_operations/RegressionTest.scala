package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._

class RegressionTest extends FunSuite with TestGraphOp {
  def predict(method: String, label: Attribute[Double], features: Seq[Attribute[Double]]) = {
    val op = Regression(method, features.size)
    val prediction = op(op.features, features)(op.label, label).result.prediction
    prediction.rdd.collect.toMap
  }

  def assertRoughly(
    result: Map[Long, Double], expectation: Map[Long, Double], maxError: Double = 0.0) = {
    assert(result.size == expectation.size)
    val error = result.map { case (k, v) => Math.abs(v - expectation(k)) }
    assert(error.max <= maxError, s"\n  $result is unlike $expectation")
  }

  def predictLabelFromAttr(
    method: String,
    label: Map[Int, Double],
    attrs: Seq[Map[Int, Double]]): Map[Long, Double] = {
    // Create the graph from attr in case of missing labels.
    val g = SmallTestGraph(attrs(0).mapValues(_ => Seq())).result
    val l = AddVertexAttribute.run(g.vs, label)
    val a = attrs.map(attr => AddVertexAttribute.run(g.vs, attr))
    predict(method, l, a)
  }

  def testRegressions(
    label: Map[Int, Double],
    attrs: Seq[Map[Int, Double]],
    expectation: Map[Long, Double],
    maxError: Double) {
    for (method <- Seq("Linear regression", "Ridge regression", "Lasso")) {
      println("       . " + method)
      assertRoughly(
        predictLabelFromAttr(method, label, attrs),
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
      Map(0L -> 1000.0, 1L -> 930.0, 2L -> 2000.0, 3L -> 390.0), maxError = 5)
    assertRoughly(incomes("Ridge regression"),
      Map(0L -> 1000.0, 1L -> 930.0, 2L -> 2000.0, 3L -> 390.0), maxError = 5)
    assertRoughly(incomes("Lasso"),
      Map(0L -> 1000.0, 1L -> 930.0, 2L -> 2000.0, 3L -> 390.0), maxError = 5)
    assertRoughly(incomes("Decision tree"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 2000.0, 3L -> 1000.0), maxError = 5)
    assertRoughly(incomes("Random forest"),
      Map(0L -> 950.0, 1L -> 950.0, 2L -> 1350.0, 3L -> 950.0), maxError = 5)
    assertRoughly(incomes("Gradient-boosted trees"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 2000.0, 3L -> 1000.0), maxError = 5)
  }

  // More like classification.
  test("gender from age") {
    def gender(method: String) = {
      println("       . " + method)
      val g = ExampleGraph()().result
      val gender = g.gender.deriveX[Double]("if (x == \"Male\") 1.0 else 0.0")
      predict(method, gender, Seq(g.age))
    }
    assertRoughly(gender("Linear regression"),
      Map(0L -> 0.7, 1L -> 0.7, 2L -> 0.8, 3L -> 0.7), maxError = 0.1)
    assertRoughly(gender("Ridge regression"),
      Map(0L -> 0.7, 1L -> 0.7, 2L -> 0.8, 3L -> 0.7), maxError = 0.1)
    assertRoughly(gender("Lasso"),
      Map(0L -> 0.7, 1L -> 0.7, 2L -> 0.8, 3L -> 0.7), maxError = 0.1)
    assertRoughly(gender("Logistic regression"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0))
    assertRoughly(gender("Naive Bayes"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.1)
    assertRoughly(gender("Decision tree"),
      Map(0L -> 1.0, 1L -> 0.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.1)
    assertRoughly(gender("Random forest"),
      Map(0L -> 0.9, 1L -> 0.5, 2L -> 0.9, 3L -> 0.8), maxError = 0.1)
    assertRoughly(gender("Gradient-boosted trees"),
      Map(0L -> 1.0, 1L -> 0.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.1)
  }

  test("Logistic regression") {
    val prediction = {
      val g = ExampleGraph()().result
      val young = g.age.deriveX[Double]("if (x < 19.0) 1.0 else 0.0")
      predict("Logistic regression", young, Seq(g.age))
    }
    assertRoughly(prediction,
      Map(0L -> 0.0, 1L -> 1.0, 2L -> 0.0, 3L -> 1.0))
  }

  test("regression - age from year of birth") {
    testRegressions(
      label = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      attrs = Seq(Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955)),
      expectation = Map(0L -> 25, 1L -> 40, 2L -> 30, 3L -> 60),
      maxError = 1)
  }

  test("regression - year of birth from age") {
    testRegressions(
      label = Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955),
      attrs = Seq(Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60)),
      expectation = Map(0L -> 1990, 1L -> 1975, 2L -> 1985, 3L -> 1955),
      maxError = 1)
  }

  test("regression - corner cases - predict constant") {
    testRegressions(
      label = Map(0 -> 5, 1 -> 5, 2 -> 5),
      attrs = Seq(Map(0 -> 10, 1 -> 50, 2 -> 100)),
      expectation = Map(0L -> 5, 1L -> 5, 2L -> 5),
      maxError = 0.1)
  }

  ignore("regression - corner cases - predict from constant") {
    // Should not throw an error. But does.
    testRegressions(
      label = Map(0 -> 1, 1 -> 2, 2 -> 3),
      attrs = Seq(Map(0 -> 5, 1 -> 5, 2 -> 5)),
      expectation = Map(0L -> 2, 1L -> 2, 2L -> 2),
      maxError = 1)
  }

  test("regression - corner cases - noisy data") {
    // y ~= 100 - 10x.
    testRegressions(
      label = Map(0 -> 91, 1 -> 79, 2 -> 72),
      attrs = Seq(Map(0 -> 1, 1 -> 2, 2 -> 3)),
      expectation = Map(0L -> 90, 1L -> 80, 2L -> 70),
      maxError = 5)
  }

  test("regression - corner cases - missing labels") {
    testRegressions(
      label = Map(0 -> 10, 1 -> 20, 2 -> 30),
      // Significantly different means and stddev with and without the extra labels.
      attrs = Seq(Map(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 10, 4 -> 20)),
      expectation = Map(0L -> 10, 1L -> 20, 2L -> 30, 3L -> 100, 4L -> 200),
      maxError = 10)
  }

  test("regression - two dimensions") {
    // z = 2x - y + 10.
    testRegressions(
      label = Map(0 -> 20, 1 -> 10, 2 -> 40, 3 -> 30),
      attrs = Seq(
        Map(0 -> 10, 1 -> 10, 2 -> 20, 3 -> 20),
        Map(0 -> 10, 1 -> 20, 2 -> 10, 3 -> 20)),
      expectation = Map(0L -> 20, 1L -> 10, 2L -> 40, 3L -> 30),
      maxError = 1)
  }

  test("regression - missing value") {
    // We only predict for points where every feature is defined.
    testRegressions(
      label = Map(0 -> 20, 1 -> 10, 2 -> 40, 3 -> 30),
      attrs = Seq(
        Map(0 -> 10, 1 -> 10, 2 -> 20, 3 -> 20),
        Map(0 -> 10, 1 -> 20, 2 -> 10)),
      expectation = Map(0L -> 20, 1L -> 10, 2L -> 40),
      maxError = 1)
  }
}
