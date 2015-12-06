package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class RegressionTest extends FunSuite with TestGraphOp {
  test("income from age") {
    val g = ExampleGraph()().result
    for (method <- "linear ridge lasso".split(" ")) {
      val prediction = {
        val op = Regression(method, 1)
        op(op.features, Seq(g.age.entity))(op.label, g.income).result.prediction
      }
      val expectation = Map(0L -> 1000.0, 1L -> 930.0, 2L -> 2000.0, 3L -> 400.0)
      val result = prediction.rdd.collect.toMap
      val error = result.map { case (k, v) => Math.abs(v - expectation(k)) }
      assert(error.max < 10)
    }
  }
}
