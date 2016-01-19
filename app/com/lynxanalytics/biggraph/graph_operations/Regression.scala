// Trains a machine learning model and uses it to generate predictions of an attribute.
//
// MLlib can use native linear algebra packages when properly configured.
// See http://spark.apache.org/docs/latest/mllib-guide.html#dependencies.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.mllib
import org.apache.spark.rdd

case class RegressionParams(
  // Labeled training data points.
  val points: rdd.RDD[mllib.regression.LabeledPoint],
  // All data points for the model to evaluate.
  val vectors: AttributeRDD[mllib.linalg.Vector],
  // An optional scaler if it was used to scale the labels. It can be used
  // to scale back the results.
  val scaler: Option[mllib.feature.StandardScalerModel])

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

    val (predictions, vectors) = method match {
      case "Linear regression" =>
        val p = getParams(forSGD = true)
        val model = new mllib.regression.LinearRegressionWithSGD().setIntercept(true).run(p.points)
        (scaleBack(model.predict(p.vectors.values), p.scaler.get), p.vectors)
      case "Ridge regression" =>
        val p = getParams(forSGD = true)
        val model = new mllib.regression.RidgeRegressionWithSGD().setIntercept(true).run(p.points)
        (scaleBack(model.predict(p.vectors.values), p.scaler.get), p.vectors)
      case "Lasso" =>
        val p = getParams(forSGD = true)
        val model = new mllib.regression.LassoWithSGD().setIntercept(true).run(p.points)
        (scaleBack(model.predict(p.vectors.values), p.scaler.get), p.vectors)
      case "Logistic regression" =>
        val p = getParams(forSGD = false)
        val model =
          new mllib.classification.LogisticRegressionWithLBFGS().setNumClasses(10).run(p.points)
        (model.predict(p.vectors.values), p.vectors)
      case "Naive Bayes" =>
        val p = getParams(forSGD = false)
        val model = mllib.classification.NaiveBayes.train(p.points)
        (model.predict(p.vectors.values), p.vectors)
      case "Decision tree" =>
        val p = getParams(forSGD = false)
        val model = mllib.tree.DecisionTree.trainRegressor(
          input = p.points,
          categoricalFeaturesInfo = Map[Int, Int](), // All continuous.
          impurity = "variance", // Options: gini, entropy, variance as of Spark 1.6.0.
          maxDepth = 5,
          maxBins = 32)
        (model.predict(p.vectors.values), p.vectors)
      case "Random forest" =>
        val p = getParams(forSGD = false)
        val model = mllib.tree.RandomForest.trainRegressor(
          input = p.points,
          categoricalFeaturesInfo = Map[Int, Int](), // All continuous.
          numTrees = 10,
          featureSubsetStrategy = "onethird",
          impurity = "variance", // Options: gini, entropy, variance as of Spark 1.6.0.
          maxDepth = 4,
          maxBins = 100,
          seed = 0)
        (model.predict(p.vectors.values), p.vectors)
      case "Gradient-boosted trees" =>
        val p = getParams(forSGD = false)
        val boostingStrategy = mllib.tree.configuration.BoostingStrategy.defaultParams("Regression")
        boostingStrategy.numIterations = 10
        boostingStrategy.treeStrategy.maxDepth = 5
        boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]() // All continuous.
        val model = mllib.tree.GradientBoostedTrees.train(p.points, boostingStrategy)
        (model.predict(p.vectors.values), p.vectors)
    }
    val ids = vectors.keys // We just put back the keys with a zip.
    output(o.prediction, ids.zip(predictions).asUniqueSortedRDD(vectors.partitioner.get))
  }

  // Transforms the result RDD using the inverse transformation of the original scaling.
  private def scaleBack(
    result: rdd.RDD[Double],
    scaler: mllib.feature.StandardScalerModel): rdd.RDD[Double] = {
    result.map { v => v * scaler.std(0) + scaler.mean(0) }
  }

  // Creates the input for trainign and evaluation.
  private def getParams(
    forSGD: Boolean // Whether the data should be prepared for an SGD method.
    )(implicit id: DataSet): RegressionParams = {
    val vertices = inputs.vertices.rdd
    val labelRDD = inputs.label.rdd

    // All scaled data points.
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
      val scaler = new mllib.feature.StandardScaler(
        withMean = forSGD, // Center the vectors for SGD training methods.
        withStd = true).fit(
        // Set the scaler based on only the training vectors, i.e. where we have a label.
        labelRDD.sortedJoin(unscaled).values.map {
          case (_, v) => v
        })
      // Scale all vectors using the scaler created from the training vectors.
      unscaled.mapValues(v => scaler.transform(v))
    }

    val (labels, labelScaler) = if (forSGD) {
      // For SGD methods the labels need to be scaled too. Otherwise the optimal stepSize can
      // vary greatly.
      val labelVector = labelRDD.mapValues {
        a => new mllib.linalg.DenseVector(Array(a)): mllib.linalg.Vector
      }
      val labelScaler = new mllib.feature.StandardScaler(withMean = true, withStd = true)
        .fit(labelVector.values)
      (labelVector.mapValues(v => labelScaler.transform(v)(0)), Some(labelScaler))
    } else {
      (labelRDD, None)
    }

    val points = labels.sortedJoin(vectors).values.map {
      case (l, v) => new mllib.regression.LabeledPoint(l, v)
    }
    points.cache
    RegressionParams(points, vectors, labelScaler)
  }
}
