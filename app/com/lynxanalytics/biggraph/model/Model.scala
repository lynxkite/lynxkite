// A helper class to handle machine learning models.
package com.lynxanalytics.biggraph.model

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.mllib
import org.apache.spark.rdd.RDD
import org.apache.spark

// A unified interface for different types of MLlib models.
trait ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double]
}

class LinearRegressionModelImpl(m: mllib.regression.GeneralizedLinearModel) extends ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double] = { m.predict(data) }
}

case class Model(
    method: String, // The training method used to create this model.
    symbolicPath: String, // The symbolic name of the HadoopFile where this model is saved.
    labelName: String, // Name of the label attribute used to train this model.
    featureNames: List[String], // The name of the feature attributes used to train this model.
    labelScaler: Option[mllib.feature.StandardScalerModel], // The scaler used to scale the labels.
    featureScaler: mllib.feature.StandardScalerModel) { // The scaler used to scale the features.

  // Loads the previously created model from the file system.
  def load(sc: spark.SparkContext): ModelImplementation = {
    val path = HadoopFile(symbolicPath).resolvedName
    method match {
      case "Linear regression" =>
        new LinearRegressionModelImpl(mllib.regression.LinearRegressionModel.load(sc, path))
      case "Ridge regression" =>
        new LinearRegressionModelImpl(mllib.regression.RidgeRegressionModel.load(sc, path))
      case "Lasso" =>
        new LinearRegressionModelImpl(mllib.regression.LassoModel.load(sc, path))
    }
  }

  // Scales back the labels if needed.
  def scaleBack(result: RDD[Double]): RDD[Double] = {
    if (labelScaler.isEmpty) {
      result
    } else {
      Model.scaleBack(result, labelScaler.get)
    }
  }
}

// Helper methods to transform and scale training and prediction data.
object Model {
  def toFE(modelName: String, modelMeta: ModelMeta): FEModel = FEModel(modelName, modelMeta.featureNames)

  def newModelFile: HadoopFile = {
    HadoopFile("DATA$") / io.ModelsDir / Timestamp.toString
  }

  def checkLinearModel(model: mllib.regression.GeneralizedLinearModel): Unit = {
    // A linear model with at least one NaN parameter will always predict NaN.
    for (w <- model.weights.toArray :+ model.intercept) {
      assert(!w.isNaN, "Failed to train a valid regression model.")
    }
  }

  // Transforms the result RDD using the inverse transformation of the original scaling.
  def scaleBack(
    result: RDD[Double],
    scaler: mllib.feature.StandardScalerModel): RDD[Double] = {
    assert(scaler.mean.size == 1, s"Invalid scaler mean size (${scaler.mean.size} instead of 1)")
    assert(scaler.std.size == 1, s"Invalid scaler std size (${scaler.std.size} instead of 1)")
    val mean = scaler.mean(0)
    val std = scaler.std(0)
    result.map { v => v * std + mean }
  }

  // Transforms features to an MLLIB compatible format.
  def toLinalgVector(
    features: Array[AttributeRDD[Double]],
    vertices: VertexSetRDD): AttributeRDD[mllib.linalg.Vector] = {
    val emptyArrays = vertices.mapValues(l => new Array[Double](features.size))
    val numberedFeatures = features.zipWithIndex
    val fullArrays = numberedFeatures.foldLeft(emptyArrays) {
      case (a, (f, i)) =>
        a.sortedJoin(f).mapValues {
          case (a, f) => a(i) = f; a
        }
    }
    fullArrays.mapValues(a => new mllib.linalg.DenseVector(a): mllib.linalg.Vector)
  }
}

case class FEModel(
  name: String,
  featureNames: List[String])

trait ModelMeta {
  def featureNames: List[String]
}

case class ScaledParams(
  // Labeled training data points.
  points: RDD[mllib.regression.LabeledPoint],
  // All feature data.
  vectors: AttributeRDD[mllib.linalg.Vector],
  // An optional scaler if it was used to scale the labels. It can be used
  // to scale back the results.
  labelScaler: Option[mllib.feature.StandardScalerModel],
  featureScaler: mllib.feature.StandardScalerModel)

class Scaler(
    // Whether the data should be prepared for a Stochastic Gradient Descent method.
    forSGD: Boolean) {

  // Creates the input for training and evaluation.
  def scale(
    labelRDD: AttributeRDD[Double],
    features: Array[AttributeRDD[Double]],
    vertices: VertexSetRDD)(implicit id: DataSet): ScaledParams = {

    val unscaled = Model.toLinalgVector(features, vertices)
    // All scaled data points.
    val (vectors, featureScaler) = {

      // Must scale the features or we get NaN predictions. (SPARK-1859)
      val scaler = new mllib.feature.StandardScaler(
        withMean = forSGD, // Center the vectors for SGD training methods.
        withStd = true).fit(
        // Set the scaler based on only the training vectors, i.e. where we have a label.
        labelRDD.sortedJoin(unscaled).values.map {
          case (_, v) => v
        })
      // Scale all vectors using the scaler created from the training vectors.
      (unscaled.mapValues(v => scaler.transform(v)), scaler)
    }

    val (labels, labelScaler) = if (forSGD) {
      // For SGD methods the labels need to be scaled too. Otherwise the optimal
      // stepSize can vary greatly.
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
    ScaledParams(points, vectors, labelScaler, featureScaler)
  }
}
