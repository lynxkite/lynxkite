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

    // Check that data points with similar ages have the same label. 
    // Check that data points with distant ages have different labels.
    val impl = m.load(sparkContext)
    val ages = vectorsRDD(Array(15), Array(20), Array(50))
    val clustering = impl.transform(
      ages.map(v => m.featureScaler.transform(v))).collect
    assert(clustering(0) == clustering(1),
      "Age 15 and Age 20 shall be in the same cluster.")
    assert(clustering(0) != clustering(2),
      "Age 15 and Age 50 shall be in the different cluster.")
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

    // Check that data points with similar values have the same label. 
    // Check that data points with distant values have different labels. 
    val impl = m.load(sparkContext)
    val data = vectorsRDD(Array.fill(numAttr)(1), Array.fill(numAttr)(5),
      Array.fill(numAttr)(96), Array.fill(numAttr)(100))
    val clustering = impl.transform(
      data.map(v => m.featureScaler.transform(v))).collect
    assert(clustering(0) == clustering(1))
    assert(clustering(2) == clustering(3))
    assert(clustering(0) != clustering(3))
  }
}
