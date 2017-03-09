// A base class for model related tests with utility methods.
package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.model._

import org.apache.spark.mllib
import org.apache.spark.rdd

class ModelTestBase extends FunSuite with TestGraphOp {
  def model(
    method: String,
    labelName: String,
    label: Map[Int, Double],
    featureNames: List[String],
    attrs: Seq[Map[Int, Double]],
    graph: SmallTestGraph.Output): Scalar[Model] = {
    val l = AddVertexAttribute.run(graph.vs, label)
    val features = attrs.map(attr => AddVertexAttribute.run(graph.vs, attr))
    method match {
      case "Linear regression" | "Ridge regression" | "Lasso" =>
        val op = RegressionModelTrainer(method, labelName, featureNames)
        op(op.features, features)(op.label, l).result.model
      case "Decision tree regression" =>
        val op = TrainDecisionTreeRegressor(
          labelName,
          featureNames,
          impurity = "variance",
          maxBins = 32,
          maxDepth = 5,
          minInfoGain = 0,
          minInstancesPerNode = 1,
          seed = 1234567)
        op(op.features, features)(op.label, l).result.model
      case "Logistic regression" =>
        val op = LogisticRegressionModelTrainer(
          maxIter = 20,
          labelName,
          featureNames)
        op(op.features, features)(op.label, l).result.model
      case "Decision tree classification" =>
        val op = TrainDecisionTreeClassifier(
          labelName,
          featureNames,
          impurity = "gini",
          maxBins = 32,
          maxDepth = 5,
          minInfoGain = 0,
          minInstancesPerNode = 1,
          seed = 1234567)
        op(op.features, features)(op.label, l).result.model
    }
  }

  def testKMeansModel(numAttr: Int, numData: Int, k: Int): Scalar[Model] = {
    // 100 data where the first data point has 20 attributes of value 1.0, the
    // second data point has 20 attributes of value 2.0 ..., and the last data
    // point has 20 attributes of value 100.0.
    val attrs = (1 to numAttr).map(i => (1 to numData).map {
      case x => x -> x.toDouble
    }.toMap)
    val g = SmallTestGraph(attrs(0).mapValues(_ => Seq()), 10).result
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val featureNames = (1 to numAttr).toList.map { i => i.toString }
    val op = KMeansClusteringModelTrainer(
      k,
      maxIter = 50,
      seed = 1000,
      featureNames)
    // The k-means model built from the above features
    op(op.features, features).result.model
  }

  def predict(m: Scalar[Model], features: Seq[Attribute[Double]]): Attribute[Double] = {
    val op = PredictFromModel(features.size)
    op(op.model, m)(op.features, features).result.prediction
  }

  def graph(numVertices: Int): SmallTestGraph.Output = {
    SmallTestGraph((0 until numVertices).map(v => v -> Seq()).toMap).result
  }

  def assertRoughlyEquals(x: Double, y: Double, maxDifference: Double): Unit = {
    assert(Math.abs(x - y) < maxDifference, s"$x does not equal to $y with $maxDifference precision")
  }

  def assertRoughlyEquals(x: Map[ID, Double], y: Map[Int, Double], maxDifference: Double): Unit = {
    x.foreach { case (i, d) => assertRoughlyEquals(d, y(i.toInt), maxDifference) }
  }

  def vectorsRDD(arr: Array[Double]*): rdd.RDD[mllib.linalg.Vector] = {
    sparkContext.parallelize(arr.map(v => new mllib.linalg.DenseVector(v)))
  }
}

object DataForDecisionTreeTests {
  // I love making long walks if it's not raining and the temperature is
  // pleasant. I take only a short walk if it's not raining, but the weather
  // is too hot or too cold. I hate rain, so I just stay at home if it's raining.
  // Sometimes I'm in a really good mood and go on a long walk in spite of
  // the cold weather.
  case class GraphData(
    labelName: String,
    label: Map[Int, Double],
    featureNames: List[String],
    attrs: Seq[Map[Int, Double]],
    probability: Map[Int, Double] = Map(),
    vertexNumber: Int)

  val trainingData = GraphData(
    labelName = "length of the walk",
    label = Map(0 -> 1, 1 -> 0, 2 -> 0, 3 -> 2, 4 -> 1, 5 -> 0, 6 -> 1, 7 -> 2),
    featureNames = List("temperature", "rain"),
    attrs = Seq(Map(0 -> -15, 1 -> 20, 2 -> -10, 3 -> 20, 4 -> 35, 5 -> 40, 6 -> -15, 7 -> -15),
      Map(0 -> 0, 1 -> 1, 2 -> 1, 3 -> 0, 4 -> 0, 5 -> 1, 6 -> 0, 7 -> 0)),
    vertexNumber = 8)
  val testDataForClassification = GraphData(
    labelName = "length of the walk",
    label = Map(0 -> 2, 1 -> 0, 2 -> 1, 3 -> 1, 4 -> 0, 5 -> 0),
    featureNames = List("temperature", "rain"),
    attrs = Seq(Map(0 -> 20.0, 1 -> 42.0, 2 -> 38.0, 3 -> -16.0, 4 -> -20.0, 5 -> 20.0),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 1.0, 5 -> 1.0)),
    probability = Map(0 -> 1, 1 -> 1, 2 -> 1, 3 -> 0.6667, 4 -> 1, 5 -> 1),
    vertexNumber = 6)
  val testDataForRegression = GraphData(
    labelName = "length of the walk",
    label = Map(0 -> 2, 1 -> 0, 2 -> 1, 3 -> 1.3333, 4 -> 0, 5 -> 0),
    featureNames = List("temperature", "rain"),
    attrs = Seq(Map(0 -> 20.0, 1 -> 42.0, 2 -> 38.0, 3 -> -16.0, 4 -> -20.0, 5 -> 20.0),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 1.0, 5 -> 1.0)),
    vertexNumber = 6)
}
