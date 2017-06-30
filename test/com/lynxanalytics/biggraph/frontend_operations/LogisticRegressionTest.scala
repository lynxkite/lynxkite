package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class LogisticRegressionTest extends OperationsTestBase {
  test("train and score a logistic regression") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "label", "expr" -> "if (age > 30) 1.0 else 0.0"))
      .box("Train a logistic regression model",
        Map("name" -> "model", "label" -> "label", "features" -> "age", "max_iter" -> "20"))
      .box("Classify vertices with a model",
        Map(
          "name" -> "classification",
          "model" -> """{
            "modelName" : "model",
            "isClassification" : true,
            "generatesProbability" : true,
            "features" : ["age"]}"""
        )
      ).project
    val classification = project.vertexAttributes("classification").runtimeSafeCast[Double]
    val classificationMap = classification.rdd.collect.toMap
    val certainty = project.vertexAttributes("classification_certainty").runtimeSafeCast[Double]
    val certaintyMap = certainty.rdd.collect.toMap
    val probabilityOf0 =
      project.vertexAttributes("classification_probability_of_0").runtimeSafeCast[Double]
    val probabilityOf0Map = probabilityOf0.rdd.collect.toMap
    val probabilityOf1 =
      project.vertexAttributes("classification_probability_of_1").runtimeSafeCast[Double]
    val probabilityOf1Map = probabilityOf1.rdd.collect.toMap

    // Example graph age: 0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.
    assert(classificationMap == Map(0 -> 0.0, 1 -> 0.0, 2 -> 1.0, 3 -> 0.0))
    assert(certaintyMap(3) > certaintyMap(1) && certaintyMap(1) > certaintyMap(0))
    classificationMap.map {
      case (id, cl) => {
        if (cl == 0) {
          assert(certaintyMap(id) == probabilityOf0Map(id))
        } else {
          assert(certaintyMap(id) == probabilityOf1Map(id))
        }
        assert(probabilityOf0Map(id) + probabilityOf1Map(id) == 1)
      }
    }
  }
}
