// A helper class to handle machine learning models.
package com.lynxanalytics.biggraph.model

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.mllib
import org.apache.spark.ml
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark
import play.api.libs.json
import play.api.libs.json.JsNull

// A unified interface for different types of MLlib models.
trait ModelImplementation {
  def transform(data: RDD[mllib.linalg.Vector]): RDD[Double]
  def details: String
}

// Helper classes to provide a common abstraction for various types of models.
private class LinearRegressionModelImpl(
    m: mllib.regression.GeneralizedLinearModel) extends ModelImplementation {
  def transform(data: RDD[mllib.linalg.Vector]): RDD[Double] = { m.predict(data) }
  def details: String = {
    val weights = "(" + m.weights.toArray.mkString(", ") + ")"
    s"intercept: ${m.intercept}\nweights: $weights"
  }
}

private class ClusterModelImpl(
    m: ml.clustering.KMeansModel,
    featureScaler: mllib.feature.StandardScalerModel) extends ModelImplementation {

  def transform(data: RDD[mllib.linalg.Vector]): RDD[Double] = {
    val dataDF = {
      val sc = data.context
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      data.map(x => Tuple1(x)).toDF("vector")
    }
    // Output a rdd of resulting cluster labels
    m.transform(dataDF).map { row => row.getAs[Int](1).toDouble }
  }
  val scaledCenters = {
    val unscaledCenters = m.clusterCenters
    val transformingVector = featureScaler.std
    val transformer = new mllib.feature.ElementwiseProduct(transformingVector)
    unscaledCenters.map(transformer.transform(_))
  }
  def details: String = {
    s"cluster centers: ${scaledCenters.mkString}"
  }
}

case class Model(
  isClassification: Boolean, // The model is a classification model or a regression model.
  method: String, // The training method used to create this model.
  symbolicPath: String, // The symbolic name of the HadoopFile where this model is saved.
  labelName: Option[String], // Name of the label attribute used to train this model.
  featureNames: List[String], // The name of the feature attributes used to train this model.
  labelScaler: Option[mllib.feature.StandardScalerModel], // The scaler used to scale the labels.
  featureScaler: mllib.feature.StandardScalerModel) // The scaler used to scale the features.
    extends ToJson with Equals {

  private def standardScalerModelToJson(model: Option[mllib.feature.StandardScalerModel]): json.JsValue = {
    if (model.isDefined) {
      json.Json.obj(
        "std" -> json.Json.parse(model.get.std.toJson),
        "mean" -> json.Json.parse(model.get.mean.toJson),
        "withStd" -> model.get.withStd,
        "withMean" -> model.get.withMean)
    } else {
      JsNull
    }
  }

  private def standardScalerModelEquals(left: mllib.feature.StandardScalerModel,
                                        right: mllib.feature.StandardScalerModel): Boolean = {
    left.mean == right.mean &&
      left.std == right.std &&
      left.withMean == right.withMean &&
      left.withStd == right.withStd
  }

  override def equals(other: Any) = {
    if (canEqual(other)) {
      val o = other.asInstanceOf[Model]
      def labelScalerEquals = ((labelScaler.isEmpty && o.labelScaler.isEmpty) ||
        standardScalerModelEquals(labelScaler.get, o.labelScaler.get))
      method == o.method &&
        isClassification == o.isClassification &&
        symbolicPath == o.symbolicPath &&
        labelName == o.labelName &&
        featureNames == o.featureNames &&
        labelScalerEquals &&
        standardScalerModelEquals(featureScaler, o.featureScaler)
    } else {
      false
    }
  }

  override def canEqual(other: Any) = other.isInstanceOf[Model]

  override def toJson: json.JsValue = {
    json.Json.obj(
      "isClassification" -> isClassification,
      "method" -> method,
      "symbolicPath" -> symbolicPath,
      "labelName" -> labelName,
      "featureNames" -> featureNames,
      "labelScaler" -> standardScalerModelToJson(labelScaler),
      "featureScaler" -> standardScalerModelToJson(Some(featureScaler))
    )
  }

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
      case "KMeans clustering" =>
        new ClusterModelImpl(ml.clustering.KMeansModel.load(path), featureScaler)
    }
  }

  def scalerDetails: String = {
    val meanInfo =
      if (featureScaler.withMean) {
        val vec = "(" + featureScaler.mean.toArray.mkString(", ") + ")"
        s"Centered to 0; original mean was $vec\n"
      } else {
        ""
      }
    val stdInfo =
      if (featureScaler.withStd) {
        val vec = "(" + featureScaler.std.toArray.mkString(", ") + ")"
        s"Scaled to unit standard deviation; original deviation was $vec"
      } else {
        ""
      }
    meanInfo + stdInfo
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
object Model extends FromJson[Model] {
  private def standardScalerModelFromJson(j: json.JsValue): Option[mllib.feature.StandardScalerModel] = {
    j match {
      case JsNull => None
      case _ =>
        val std = org.apache.spark.mllib.linalg.Vectors.fromJson(json.Json.stringify(j \ "std"))
        val mean = org.apache.spark.mllib.linalg.Vectors.fromJson(json.Json.stringify(j \ "mean"))
        val withStd = (j \ "withStd").as[Boolean]
        val withMean = (j \ "withMean").as[Boolean]
        Some(new mllib.feature.StandardScalerModel(std, mean, withStd, withMean))
    }
  }

  override def fromJson(j: json.JsValue): Model = {
    Model(
      (j \ "isClassification").as[Boolean],
      (j \ "method").as[String],
      (j \ "symbolicPath").as[String],
      (j \ "labelName").as[Option[String]],
      (j \ "featureNames").as[List[String]],
      standardScalerModelFromJson(j \ "labelScaler"),
      standardScalerModelFromJson(j \ "featureScaler").get
    )
  }
  def toMetaFE(modelName: String, modelMeta: ModelMeta): FEModelMeta = FEModelMeta(
    modelName, modelMeta.isClassification, modelMeta.featureNames)

  def toFE(m: Model, sc: spark.SparkContext): FEModel = FEModel(
    method = m.method,
    labelName = m.labelName,
    featureNames = m.featureNames,
    scalerDetails = m.scalerDetails,
    details = m.load(sc).details)

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

case class FEModelMeta(
  name: String,
  isClassification: Boolean,
  featureNames: List[String])

case class FEModel(
  method: String,
  labelName: Option[String],
  featureNames: List[String],
  scalerDetails: String,
  details: String)

trait ModelMeta {
  def isClassification: Boolean
  def featureNames: List[String]
}

case class ScaledParams(
  // Labeled training data points.
  points: Option[RDD[mllib.regression.LabeledPoint]],
  // All feature data.
  vectors: AttributeRDD[mllib.linalg.Vector],
  // An optional scaler if it was used to scale the labels. It can be used
  // to scale back the results.
  labelScaler: Option[mllib.feature.StandardScalerModel],
  featureScaler: mllib.feature.StandardScalerModel)

class Scaler(
    // Whether the data should be prepared for a Stochastic Gradient Descent method.
    // TODO: Add more conditions in the future because not all scalers depend only on SGD.
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

    val points = Some(labels.sortedJoin(vectors).values.map {
      case (l, v) => new mllib.regression.LabeledPoint(l, v)
    })
    points.get.cache
    ScaledParams(points, vectors, labelScaler, featureScaler)
  }

  // This feature scaler can be used for unsupervised learning.
  def scaleFeatures(
    features: Array[AttributeRDD[Double]],
    vertices: VertexSetRDD)(implicit id: DataSet): ScaledParams = {

    val unscaled = Model.toLinalgVector(features, vertices)
    // All scaled data points.
    val (vectors, featureScaler) = {

      val scaler = new mllib.feature.StandardScaler(
        withMean = forSGD, // Center the vectors for SGD training methods.
        withStd = true)
        .fit(unscaled.values)
      // Scale all vectors using the scaler created from the training vectors.
      (unscaled.mapValues(v => scaler.transform(v)), scaler)
    }
    ScaledParams(points = None, vectors, labelScaler = None, featureScaler)
  }
}
