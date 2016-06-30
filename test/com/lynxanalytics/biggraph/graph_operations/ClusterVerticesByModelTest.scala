package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ClusterVerticesByModelTest extends ModelTestBase {
  test("test clustering by model") {
    val g = ExampleGraph()().result
    val featureNames = List("age")
    val features: Seq[Attribute[Double]] = Seq(g.age)
    val op = KMeansClusteringModelTrainer(2, 20, 0.0001, 1000, featureNames)
    val m = op(op.features, features).result.model

    val op2 = ClusterVerticesByModel(1)
    val result = op2(op2.features, features)(op2.model, m).result
    val clustering = result.clustering.rdd
    assert(clustering.count == 4)
    assert(clustering.lookup(0) == clustering.lookup(1) &&
      clustering.lookup(0) == clustering.lookup(3))
  }

  test("larger data set with 20 attributes") {
    val numAttr = 20
    val numData = 100
    // 100 data where the first data point has twenty attributes of value 1.0, the 
    // second data point has twenty attributes of value 2.0 ... etc 
    val attrs = (1 to numAttr).map(i => (1 to numData).map {
      case x => x -> x.toDouble
    }.toMap)
    val g = SmallTestGraph(attrs(0).mapValues(_ => Seq()), 10).result
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val featureNames = (1 to 20).toList.map { i => i.toString }
    val op = KMeansClusteringModelTrainer(10, 50, 0.0001, 1000, featureNames)
    val m = op(op.features, features).result.model
    // 100 data where the first data point has twenty attributes of value 100.0, 
    // the second data point has twenty attributes of value 99.0 ... etc
    val attrs2 = (1 to numAttr).map(i => (1 to numData).map {
      case x => x -> (numData - x + 1).toDouble
    }.toMap)
    val features2 = attrs2.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val op2 = ClusterVerticesByModel(numAttr)
    val result = op2(op2.features, features2)(op2.model, m).result
    val clustering = result.clustering.rdd
    assert(clustering.count == numData)
    // Check the first five and the last five data points shall have same labels  
    assert(clustering.lookup(1) == clustering.lookup(5))
    assert(clustering.lookup(numData - 4) == clustering.lookup(numData))
  }
}
