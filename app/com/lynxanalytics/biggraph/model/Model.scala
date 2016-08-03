// A helper class to handle machine learning models.
package com.lynxanalytics.biggraph.model

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.mllib
import org.apache.spark.ml
import org.apache.spark.rdd.RDD
import org.apache.spark
import play.api.libs.json
import play.api.libs.json.JsNull

object Implicits {
  // Easy access to the ModelMeta class from Scalar[Model].
  implicit class ModelMetaConverter(self: Scalar[Model]) {
    def modelMeta = self.source.operation.asInstanceOf[ModelMeta]
  }
}

// A unified interface for different types of MLlib models.
trait ModelImplementation {
  // A transformation of dataframe with the model. 
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame
  def details: String
}

// Helper classes to provide a common abstraction for various types of models.
private[biggraph] class LinearRegressionModelImpl(
    m: ml.regression.LinearRegressionModel,
    statistics: String) extends ModelImplementation {
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
}

private[biggraph] class LogisticRegressionModelImpl(
    m: ml.classification.LogisticRegressionModel,
    statistics: String) extends ModelImplementation {
  // Transform the data with logistic regression model to a dataframe with the schema [vector |
  // rawPredition | probability | prediction].
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
  def getThreshold: Double = m.getThreshold
}

private[biggraph] class ClusterModelImpl(
    m: ml.clustering.KMeansModel, statistics: String,
    featureScaler: mllib.feature.StandardScalerModel) extends ModelImplementation {
  // Transform the data with clustering model. 
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = {
    val scaledCenters = "(" + {
      val unscaledCenters = m.clusterCenters
      val transformingVector = featureScaler.std
      val transformer = new mllib.feature.ElementwiseProduct(transformingVector)
      unscaledCenters.map(transformer.transform(_))
    }.mkString(", ") + ")"
    s"cluster centers: ${scaledCenters}\n" + statistics
  }
}

case class Model(
  method: String, // The training method used to create this model.
  symbolicPath: String, // The symbolic name of the HadoopFile where this model is saved.
  labelName: Option[String], // Name of the label attribute used to train this model.
  featureNames: List[String], // The name of the feature attributes used to train this model.
  featureScaler: Option[mllib.feature.StandardScalerModel], // The scaler used to scale the features.
  statistics: Option[String]) // For the details that require training data 
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
      def featureScalerEquals = ((featureScaler.isEmpty && o.featureScaler.isEmpty) ||
        standardScalerModelEquals(featureScaler.get, o.featureScaler.get))
      method == o.method &&
        symbolicPath == o.symbolicPath &&
        labelName == o.labelName &&
        featureNames == o.featureNames &&
        featureScalerEquals
    } else {
      false
    }
  }

  override def canEqual(other: Any) = other.isInstanceOf[Model]

  override def toJson: json.JsValue = {
    json.Json.obj(
      "method" -> method,
      "symbolicPath" -> symbolicPath,
      "labelName" -> labelName,
      "featureNames" -> featureNames,
      "featureScaler" -> standardScalerModelToJson(featureScaler),
      "statistics" -> statistics
    )
  }

  // Loads the previously created model from the file system.
  def load(sc: spark.SparkContext): ModelImplementation = {
    val path = HadoopFile(symbolicPath).resolvedName
    method match {
      case "Linear regression" | "Ridge regression" | "Lasso" =>
        new LinearRegressionModelImpl(ml.regression.LinearRegressionModel.load(path), statistics.get)
      case "Logistic regression" =>
        new LogisticRegressionModelImpl(ml.classification.LogisticRegressionModel.load(path), statistics.get)
      case "KMeans clustering" =>
        new ClusterModelImpl(ml.clustering.KMeansModel.load(path), statistics.get, featureScaler.get)
    }
  }

  def scaleFeatures(unscaledFeatures: AttributeRDD[mllib.linalg.Vector]): AttributeRDD[mllib.linalg.Vector] = {
    if (featureScaler.isEmpty) {
      unscaledFeatures
    } else {
      unscaledFeatures.mapValues(featureScaler.get.transform(_))
    }
  }

  def scalerDetails: String = {
    if (featureScaler.isEmpty) {
      ""
    } else {
      val featureScalerValue = featureScaler.get
      val meanInfo =
        if (featureScalerValue.withMean) {
          val vec = "(" + featureScalerValue.mean.toArray.mkString(", ") + ")"
          s"Centered to 0; original mean was $vec\n"
        } else {
          ""
        }
      val stdInfo =
        if (featureScalerValue.withStd) {
          val vec = "(" + featureScalerValue.std.toArray.mkString(", ") + ")"
          s"Scaled to unit standard deviation; original deviation was $vec"
        } else {
          ""
        }
      meanInfo + stdInfo
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
      (j \ "method").as[String],
      (j \ "symbolicPath").as[String],
      (j \ "labelName").as[Option[String]],
      (j \ "featureNames").as[List[String]],
      standardScalerModelFromJson(j \ "featureScaler"),
      (j \ "statistics").as[Option[String]]
    )
  }
  def toMetaFE(modelName: String, modelMeta: ModelMeta): FEModelMeta = FEModelMeta(
    modelName, modelMeta.isClassification, modelMeta.generatesProbability, modelMeta.featureNames)

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
    featuresArray: Array[AttributeRDD[Double]],
    vertices: VertexSetRDD): AttributeRDD[mllib.linalg.Vector] = {
    val emptyArrays = vertices.mapValues(l => new Array[Double](featuresArray.size))
    val numberedFeatures = featuresArray.zipWithIndex
    val fullArrays = numberedFeatures.foldLeft(emptyArrays) {
      case (a, (f, i)) =>
        a.sortedJoin(f).mapValues {
          case (a, f) => a(i) = f; a
        }
    }
    fullArrays.mapValues(a => new mllib.linalg.DenseVector(a): mllib.linalg.Vector)
  }

  def getMAPE(predictionAndLabels: spark.sql.DataFrame): Double = {
    predictionAndLabels.map {
      row =>
        {
          val prediction = row.getDouble(0)
          val label = row.getDouble(1)
          if (prediction == label) {
            0.0
            // Return an error of 100% if a zero division error occurs.
          } else if (prediction == 0.0) {
            1.0
          } else {
            math.abs(prediction / label - 1.0)
          }
        }
    }.mean * 100.0
  }
}

// Helper method to print statistical tables of the models.
object Tabulator {
  def getTable(
    headers: Array[String],
    rowNames: Array[String],
    columnData: Array[Array[Double]]): String = {
    assert(rowNames.size == columnData(0).size)
    val tails = rowNames +: columnData.map(_.map(x => f"$x%1.6f"))
    assert(headers.size == tails.size)
    format(headers +: tails.transpose)
  }

  def format(table: Array[Array[String]]): String = {
    val colSizes = table.transpose.map(_.map(_.length).max)
    val dataAndSizes = table.map(_.zip(colSizes))
    dataAndSizes.map {
      row =>
        row.map {
          case (data, size) => ("%" + size + "s").format(data)
        }.mkString("  ")
    }.mkString("", "\n", "\n")
  }
}

case class FEModelMeta(
  name: String,
  isClassification: Boolean,
  generatesProbability: Boolean,
  featureNames: List[String])

case class FEModel(
  method: String,
  labelName: Option[String],
  featureNames: List[String],
  scalerDetails: String,
  details: String)

trait ModelMeta {
  def isClassification: Boolean
  def generatesProbability: Boolean = false
  def featureNames: List[String]
}

case class ScaledParams(
  // Labeled training data points.
  labeledPoints: Option[RDD[mllib.regression.LabeledPoint]],
  // All feature data.
  features: AttributeRDD[mllib.linalg.Vector],
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
    featuresArray: Array[AttributeRDD[Double]],
    vertices: VertexSetRDD)(implicit id: DataSet): ScaledParams = {

    val unscaled = Model.toLinalgVector(featuresArray, vertices)
    // All scaled data points.
    val (features, featureScaler) = {

      // Must scale the features or we get NaN predictions. (SPARK-1859)
      val scaler = new mllib.feature.StandardScaler(
        withMean = forSGD, // Center the features for SGD training methods.
        withStd = true).fit(
        // Set the scaler based on only the training features, i.e. where we have a label.
        labelRDD.sortedJoin(unscaled).values.map {
          case (_, v) => v
        })
      // Scale all features using the scaler created from the training features.
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

    val labeledPoints = Some(labels.sortedJoin(features).values.map {
      case (l, v) => new mllib.regression.LabeledPoint(l, v)
    })
    labeledPoints.get.cache
    ScaledParams(labeledPoints, features, labelScaler, featureScaler)
  }

  // This feature scaler can be used for unsupervised learning.
  def scaleFeatures(
    featuresArray: Array[AttributeRDD[Double]],
    vertices: VertexSetRDD)(implicit id: DataSet): ScaledParams = {

    val unscaled = Model.toLinalgVector(featuresArray, vertices)
    // All scaled data points.
    val (features, featureScaler) = {

      val scaler = new mllib.feature.StandardScaler(
        withMean = forSGD, // Center the features for SGD training methods.
        withStd = true)
        .fit(unscaled.values)
      // Scale all features using the scaler created from the training features.
      (unscaled.mapValues(v => scaler.transform(v)), scaler)
    }
    ScaledParams(labeledPoints = None, features, labelScaler = None, featureScaler)
  }
}
