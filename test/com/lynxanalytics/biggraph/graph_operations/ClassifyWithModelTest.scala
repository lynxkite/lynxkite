package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ClassifyWithModelTest extends ModelTestBase {
  test("test the k-means clustering model on larger data set with 20 attributes") {
    val numAttr = 20
    val numData = 100
    val k = 10
    val m = testKMeansModel(numAttr, numData, k) // From the ModelTestBase class.

    // A 4 vertices graph where the first data point has 20 attributes of value 100.0,
    // the second data point has 20 attributes of value 96.0, the third data point has
    // 20 attributes of value 5.0 and the last data point has 20 attributes of 1.0.
    val g = graph(numVertices = 4)
    val attrs = (0 until numAttr).map(_ => Map(0 -> 100.0, 1 -> 96.0, 2 -> 5.0, 3 -> 1.0))
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val op = ClassifyWithModel(numAttr)
    val result = op(op.features, features)(op.model, m).result
    val clustering = result.classification.rdd.values.collect
    assert(clustering.size == 4)
    // Check that the first two and the last two data points shall have identical labels.
    assert(clustering(0) == clustering(1))
    assert(clustering(2) == clustering(3))
    // Check that distant data points have different labels.
    assert(clustering(0) != clustering(3))
  }

  test("test the logistic regression model") {
    val m = model(
      method = "Logistic regression",
      labelName = "isSenior",
      label = Map(0 -> 0, 1 -> 1, 2 -> 0, 3 -> 1),
      featureNames = List("age"),
      attrs = Seq(Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60)),
      graph(4))

    val g = graph(numVertices = 4)
    val attrs = (0 until 1).map(_ => Map(0 -> 15.0, 1 -> 20.0, 2 -> 50.0, 3 -> 60.0))
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val op = ClassifyWithModel(1)
    val result = op(op.features, features)(op.model, m).result
    val classification = result.classification.rdd.values.collect
    assert(classification.size == 4)
    // Check data points with similar ages have the same label.
    assert(classification(0) == 0.0 && classification(1) == 0.0)
    assert(classification(2) == 1.0 && classification(3) == 1.0)
    val probability = result.probability.rdd.values.collect
    // Check that the probability is higher if the datapoint is farther from the threshold.
    assert(probability(0) > probability(1))
    assert(probability(3) > probability(2))

  }

  test("test the decision tree classification model") {
    val m = model(
      method = "Decision tree classification",
      labelName = "length of the walk",
      label = Map(0 -> 1, 1 -> 0, 2 -> 0, 3 -> 2, 4 -> 1, 5 -> 0, 6 -> 1, 7 -> 2),
      featureNames = List("temperature", "rain"),
      attrs = Seq(Map(0 -> -15, 1 -> 20, 2 -> -10, 3 -> 20, 4 -> 35, 5 -> 40, 6 -> -15, 7 -> -15),
        Map(0 -> 0, 1 -> 1, 2 -> 1, 3 -> 0, 4 -> 0, 5 -> 1, 6 -> 0, 7 -> 0)),
      // You can find the description of the example in TrainDecisionTreeClassifierTest.scala.
      graph(8))

    val g = graph(numVertices = 6)
    val attrs = Seq(Map(0 -> 20.0, 1 -> 42.0, 2 -> 38.0, 3 -> -16.0, 4 -> -20.0, 5 -> 20.0),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 1.0, 5 -> 1.0))
    val features = attrs.map(attr => {
      AddVertexAttribute.run[Double](g.vs, attr)
    })
    val op = ClassifyWithModel(2)
    val result = op(op.features, features)(op.model, m).result
    val classification = result.classification.rdd.values.collect
    assert(classification.size == 6)
    assert(classification(0) == 2)
    assert(classification(1) == 0)
    assert(classification(2) == 1)
    assert(classification(3) == 1)
    assert(classification(4) == 0)
    assert(classification(5) == 0)
    val probability = result.probability.rdd.values.collect
    assert(probability(0) == 1)
    assert(probability(1) == 1)
    assert(probability(2) == 1)
    assert(0.66 < probability(3) && probability(3) < 0.67)
    assert(probability(4) == 1)
    assert(probability(5) == 1)

  }
}
