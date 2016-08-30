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
      Map(
        "name" -> "classification",
        "model" -> """{
            "modelName" : "model", 
            "isClassification" : true, 
            "generatesProbability" : true, 
            "features" : ["age"]}"""
      )
    )
    val classification = project.vertexAttributes("classification").runtimeSafeCast[Double]
    val probability = project.vertexAttributes("classification_probability").runtimeSafeCast[Double]
    val probabilityMap = probability.rdd.collect.toMap
    // Example graph age: 0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.
    assert(classification.rdd.collect.toMap == Map(0 -> 0.0, 1 -> 0.0, 2 -> 1.0, 3 -> 0.0))
    assert(probabilityMap(3) > probabilityMap(1) && probabilityMap(1) > probabilityMap(0))
  }
}
