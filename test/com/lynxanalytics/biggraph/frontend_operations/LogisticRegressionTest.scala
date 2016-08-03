package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class LogisticRegressionTest extends OperationsTestBase {
  test("train and score a logistic regression") {
    run("Example Graph")
    run("Derived vertex attribute",
      Map("type" -> "double", "output" -> "label", "expr" -> "age > 30? 1.0 : 0.0"))
    run("Train a logistic regression model",
      Map("name" -> "model", "label" -> "label", "features" -> "age", "max-iter" -> "20"))
    run("Classify vertices with a model",
      Map("name" -> "classification", "model" -> "model"))
  }
}
