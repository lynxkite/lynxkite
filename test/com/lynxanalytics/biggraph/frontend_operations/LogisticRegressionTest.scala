package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class LogisticRegressionTest extends OperationsTestBase {
  test("train and score a logistic regression") {
    val project = box("Create vertices", Map("size" -> "100"))
      .box("Add random vertex attribute") // Uses a fixed seed, so the test is deterministic.
      .box(
        "Derive vertex attribute",
        Map("output" -> "label", "expr" -> "if (random > 0.5) 1.0 else 0.0"))
      .box(
        "Train a logistic regression model",
        Map(
          "name" -> "model",
          "label" -> "label",
          "features" -> "random",
          "max_iter" -> "5",
          "elastic_net_param" -> "0.8",
          "reg_param" -> "0.3"),
      )
      .box(
        "Classify with model",
        Map(
          "name" -> "classification",
          "model" -> """{
            "modelName" : "model",
            "isClassification" : true,
            "generatesProbability" : true,
            "features" : ["random"]}""",
        ),
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
    var class1 = 0
    classificationMap.foreach {
      case (id, cl) => {
        class1 += cl.toInt
        if (cl == 0) {
          assert(certaintyMap(id) == probabilityOf0Map(id))
        } else {
          assert(certaintyMap(id) == probabilityOf1Map(id))
        }
        assert(probabilityOf0Map(id) + probabilityOf1Map(id) == 1)
      }
    }
    assert(30 < class1 && class1 < 70)
  }
}
