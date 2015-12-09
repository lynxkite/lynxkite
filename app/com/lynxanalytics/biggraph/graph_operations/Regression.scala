// Trains a machine learning model and uses it to generate predictions of an attribute.
//
// MLlib can use native linear algebra packages when properly configured.
// See http://spark.apache.org/docs/latest/mllib-guide.html#dependencies.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.mllib

object Regression extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val label = vertexAttribute[Double](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val prediction = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = Regression((j \ "method").as[String], (j \ "numFeatures").as[Int])
}
import Regression._
case class Regression(method: String, numFeatures: Int) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(numFeatures)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("method" -> method, "numFeatures" -> numFeatures)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vertices = inputs.vertices.rdd
    val labels = inputs.label.rdd
    val vectors = {
      val emptyArrays = vertices.mapValues(l => new Array[Double](numFeatures))
      val numberedFeatures = inputs.features.zipWithIndex
      val fullArrays = numberedFeatures.foldLeft(emptyArrays) {
        case (a, (f, i)) =>
          a.sortedJoin(f.rdd).mapValues {
            case (a, f) => a(i) = f; a
          }
      }
      val unscaled = fullArrays.mapValues(a => new mllib.linalg.DenseVector(a): mllib.linalg.Vector)
      // Must scale the features or we get NaN predictions. (SPARK-1859)
      val scaler = new mllib.feature.StandardScaler().fit(unscaled.values)
      unscaled.mapValues(v => scaler.transform(v))
    }
    val points = labels.sortedJoin(vectors).values.map {
      case (l, v) => new mllib.regression.LabeledPoint(l, v)
    }
    points.cache

    val predictions = method match {
      case "Linear regression" =>
        val model = new mllib.regression.LinearRegressionWithSGD().setIntercept(true).run(points)
        model.predict(vectors.values)
      case "Ridge regression" =>
        val model = new mllib.regression.RidgeRegressionWithSGD().setIntercept(true).run(points)
        model.predict(vectors.values)
      case "Lasso" =>
        val model = new mllib.regression.LassoWithSGD().setIntercept(true).run(points)
        model.predict(vectors.values)
      case "Logistic regression" =>
        val model =
          new mllib.classification.LogisticRegressionWithLBFGS().setNumClasses(10).run(points)
        model.predict(vectors.values)
      case "Naive Bayes" =>
        val model = mllib.classification.NaiveBayes.train(points)
        model.predict(vectors.values)
      case "Decision tree" =>
        val model = mllib.tree.DecisionTree.trainRegressor(
          input = points,
          categoricalFeaturesInfo = Map[Int, Int](), // All continuous.
          impurity = "variance", // This is the only option at the moment.
          maxDepth = 5,
          maxBins = 32)
        model.predict(vectors.values)
      case "Random forest" =>
        val model = mllib.tree.RandomForest.trainRegressor(
          input = points,
          categoricalFeaturesInfo = Map[Int, Int](), // All continuous.
          numTrees = 10,
          featureSubsetStrategy = "onethird",
          impurity = "variance", // This is the only option at the moment.
          maxDepth = 4,
          maxBins = 100,
          seed = 0)
        model.predict(vectors.values)
      case "Gradient-boosted trees" =>
        val boostingStrategy = mllib.tree.configuration.BoostingStrategy.defaultParams("Regression")
        boostingStrategy.numIterations = 10
        boostingStrategy.treeStrategy.maxDepth = 5
        boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]() // All continuous.
        val model = mllib.tree.GradientBoostedTrees.train(points, boostingStrategy)
        model.predict(vectors.values)
    }
    val ids = vectors.keys // We just put back the keys with a zip.
    output(o.prediction, ids.zip(predictions).asUniqueSortedRDD(vectors.partitioner.get))
  }
}
