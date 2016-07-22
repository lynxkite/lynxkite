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
    // Check that the first five and the last five data points shall have same labels.
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
    assert(classification(0) == "0.0" && classification(1) == "0.0")
    assert(classification(2) == "1.0" && classification(3) == "1.0")
    val probability = result.probability.rdd.values.collect
    // Check that each probability is proportional to their attribute values and each  
    // probability is greater than 0.5.  
    assert(probability(0) > probability(1) && probability(1) > 0.5)
    assert(probability(3) > probability(2) && probability(2) > 0.5)
  }
}
