package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DecisionTreeTest extends OperationsTestBase {
  test("train and predict with a decision tree classification model") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("type" -> "Double", "output" -> "label", "expr" -> "age > 30? 2.0 : age > 15 ? 1.0 : 0.0 "))
      .box("Train a decision tree classification model",
        Map("name" -> "model",
          "label" -> "label",
          "features" -> "age",
          "impurity" -> "gini",
          "maxBins" -> "32",
          "maxDepth" -> "5",
          "minInfoGain" -> "0",
          "minInstancesPerNode" -> "1",
          "seed" -> "1234567"))
      .box("Classify vertices with a model",
        Map(
          "name" -> "classification",
          "model" -> """{
            "modelName" : "model",
            "isClassification" : true,
            "generatesProbability" : true,
            "features" : ["age"]}"""))
      .project
    val classification = project.vertexAttributes("classification").runtimeSafeCast[Double]
    val classificationMap = classification.rdd.collect.toMap
    val certainty = project.vertexAttributes("classification_certainty").runtimeSafeCast[Double]
    val certaintyMap = certainty.rdd.collect.toMap

    // Example graph age: 0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.
    assert(classificationMap == Map(0 -> 1.0, 1 -> 1.0, 2 -> 2.0, 3 -> 0.0))
    assert(certaintyMap(0) == 1)
  }

  test("train and predict with a decision tree regression model") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("type" -> "Double", "output" -> "isJoe", "expr" -> "name == 'Isolated Joe'"))
      .box("Derive vertex attribute",
        Map("type" -> "Double", "output" -> "gender01", "expr" -> "gender == 'Male' ? 0.0 : 1.0 "))
      .box("Train a decision tree regression model",
        Map("name" -> "model",
          "label" -> "age",
          "features" -> "gender01,isJoe",
          "maxBins" -> "32",
          "maxDepth" -> "5",
          "minInfoGain" -> "0",
          "minInstancesPerNode" -> "1",
          "seed" -> "1234567"))
      .box("Predict from model",
        Map(
          "name" -> "prediction",
          "model" -> """{
            "modelName" : "model",
            "isClassification" : false,
            "generatesProbability" : false,
            "features" : ["gender01","isJoe"]}"""))
      .project
    val prediction = project.vertexAttributes("prediction").runtimeSafeCast[Double]
    val predictionMap = prediction.rdd.collect.toMap
    // Example graph age: 0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.
    // Vertex 3 is Isolated Joe.
    assert(predictionMap(0) > 18.2)
    assert(predictionMap(1) > 2 && predictionMap(1) < 20.3)
    assert(predictionMap(2) > 18.2)
    assert(predictionMap(3) == 2)
  }
}
