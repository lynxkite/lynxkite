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
    val op = KMeansClusteringModelTrainer(2, 20, 0.0001, 1000, featureNames)
    val m = op(op.features, features).result.model.value

    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(m.method == "KMeans clustering")
    assert(m.featureNames == List("age"))
    assert(KMeansModel.load(path).clusterCenters.size == 2)

    //check data points with similar ages have the same label
    val impl = m.load(sparkContext)
    val ageSimilar = vectorsRDD(Array(Array(20.3), Array(18.2)))
    val clustering = impl.transform(
      ageSimilar.map(v => m.featureScaler.get.transform(v))).collect
    assert(clustering(0) == clustering(1))
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
    val m = op(op.features, features).result.model.value

    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(m.featureNames == featureNames)
    assert(KMeansModel.load(path).clusterCenters.size == 10)

    val impl = m.load(sparkContext)
    // check input data with similar attribute values withh belong to the same group 
    val dataSimilar = vectorsRDD(Array(Array.fill(20)(1), Array.fill(20)(5),
      Array.fill(20)(96), Array.fill(20)(100)))
    val clustering = impl.transform(
      dataSimilar.map(v => m.featureScaler.get.transform(v))).collect
    assert(clustering(0) == clustering(1))
    assert(clustering(2) == clustering(3))
  }
}
