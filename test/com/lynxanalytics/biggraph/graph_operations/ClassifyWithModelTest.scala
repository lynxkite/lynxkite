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
    // Check that the first five and the last five data points shall have same labels
    assert(clustering(0) == clustering(1))
    assert(clustering(2) == clustering(3))
    // Check that distant data points have different labels 
    assert(clustering(0) != clustering(3))
  }
}
