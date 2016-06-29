package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.model._

class KMeansModelTrainerTest extends ModelTestBase {

  test("example graph by age") {
    val sqlContext = dataManager.newSQLContext()
    import sqlContext.implicits._

    val g = ExampleGraph()().result
    val featureNames = List("age")
    val features = featureNames.map { name => g.age.runtimeSafeCast[Double] }
    val op = KMeansModelTrainer(2, 20, 0.0001, 1000, featureNames)
    val m = op(op.features, Seq(g.age): Seq[Attribute[Double]]).result.model.value

    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(m.method == "KMeans clustering")
    assert(m.featureNames == List("age"))
    assert(KMeansModel.load(path).clusterCenters.size == 2)

    //check data points with similar ages have the same label
    val impl = m.load(sparkContext)
    val ageSimilar = vectorsRDD(Array(Array(20.3), Array(18.2)))
    val clusterLabel = impl.transform(
      ageSimilar.map(v => m.featureScaler.get.transform(v))).collect
    assert(clusterLabel(0) == clusterLabel(1))
  }

  test("larger data set with 20 attributes") {
    val numAttr = 20
    val attrs = (1 to numAttr).map(i => (0 to 1000).map {
      case x => x -> i.toDouble
    }.toMap)
    val g = SmallTestGraph(attrs(0).mapValues(_ => Seq()), 10).result
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val featureNames = (1 to 20).toList.map { i => i.toString }
    val op = KMeansModelTrainer(10, 50, 0.0001, 1000, featureNames)
    val m = op(op.features, features).result.model.value

    val symbolicPath = m.symbolicPath
    val path = HadoopFile(symbolicPath).resolvedName
    assert(m.featureNames == featureNames)
    assert(KMeansModel.load(path).clusterCenters.size == 10)

    val impl = m.load(sparkContext)
    val dataSimilar = vectorsRDD(Array(Array.fill(20)(0), Array.fill(20)(1)))
    val clusterLabel = impl.transform(
      dataSimilar.map(v => m.featureScaler.get.transform(v))).collect
    assert(clusterLabel(0) == clusterLabel(1))
  }
}
