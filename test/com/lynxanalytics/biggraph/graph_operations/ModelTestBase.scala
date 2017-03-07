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

  def vectorsRDD(arr: Array[Double]*): rdd.RDD[mllib.linalg.Vector] = {
    sparkContext.parallelize(arr.map(v => new mllib.linalg.DenseVector(v)))
  }
}
