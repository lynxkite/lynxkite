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
    val error = result.map { case (k, v) => Math.abs(v - expectation(k)) }
    assert(error.max <= maxError, s"$result is unlike $expectation")
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
      Map(0L -> 1000.0, 1L -> 930.0, 2L -> 2000.0, 3L -> 400.0), maxError = 10)
    assertRoughly(incomes("Lasso"),
      Map(0L -> 1000.0, 1L -> 930.0, 2L -> 2000.0, 3L -> 400.0), maxError = 10)
    assertRoughly(incomes("Naive Bayes"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 1000.0, 3L -> 1000.0), maxError = 10)
    assertRoughly(incomes("Decision tree"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 2000.0, 3L -> 1000.0), maxError = 10)
    // We're just happy these give some results. They are very random on this example.
    assertRoughly(incomes("Random forest"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 2000.0, 3L -> 1000.0), maxError = 1000)
    assertRoughly(incomes("Gradient-boosted trees"),
      Map(0L -> 1000.0, 1L -> 1000.0, 2L -> 2000.0, 3L -> 1000.0), maxError = 1000)
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
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.5)
    assertRoughly(gender("Ridge regression"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.5)
    assertRoughly(gender("Lasso"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.5)
    assertRoughly(gender("Logistic regression"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.5)
    assertRoughly(gender("Naive Bayes"),
      Map(0L -> 1.0, 1L -> 1.0, 2L -> 1.0, 3L -> 1.0), maxError = 0.5)
    assertRoughly(gender("Decision tree"),
      Map(0L -> 1.0, 1L -> 0.0, 2L -> 1.0, 3L -> 0.5), maxError = 0.5)
    assertRoughly(gender("Random forest"),
      Map(0L -> 1.0, 1L -> 0.0, 2L -> 1.0, 3L -> 0.5), maxError = 0.5)
    assertRoughly(gender("Gradient-boosted trees"),
      Map(0L -> 1.0, 1L -> 0.0, 2L -> 1.0, 3L -> 0.5), maxError = 0.5)
  }
}
