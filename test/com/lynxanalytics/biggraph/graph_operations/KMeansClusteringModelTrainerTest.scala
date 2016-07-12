package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.ml.clustering.KMeansModel
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class KMeansClusteringModelTrainerTest extends ModelTestBase {

  test("example graph by age") {
    val g = ExampleGraph()().result
    val featureNames = List("age")
    val features: Seq[Attribute[Double]] = Seq(g.age)
    val op = KMeansClusteringModelTrainer(
      k = 2,
      maxIter = 20,
      seed = 1000,
      featureNames)
    val m = op(op.features, features).result.model.value

    // Load the model class and check each of its fields. 
    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(m.method == "KMeans clustering")
    assert(m.featureNames == List("age"))
    assert(KMeansModel.load(path).clusterCenters.size == 2)
  }

  test("larger data set with 20 attributes") {
    // Load the model class and check each of its fields.
    val numAttr = 20
    val numData = 100
    val k = 10
    val m = testKMeansModel(numAttr, numData, k).value // From the ModelTestBase class.
    // Load the model class and check the number of clusters.
    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(KMeansModel.load(path).clusterCenters.size == k)
  }
}
