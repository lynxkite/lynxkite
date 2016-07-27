// Trains a logistic regression model. Currently, the class only supports binary 
// classification.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model
import com.lynxanalytics.biggraph.model._
import org.apache.spark.sql
import org.apache.spark.ml
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib
import breeze.linalg

object LogisticRegressionModelTrainer extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val label = vertexAttribute[String](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val model = scalar[Model]
  }
  def fromJson(j: JsValue) = LogisticRegressionModelTrainer(
    (j \ "maxIter").as[Int],
    (j \ "labelName").as[String],
    (j \ "featureNames").as[List[String]])
}
import LogisticRegressionModelTrainer._
case class LogisticRegressionModelTrainer(
    maxIter: Int,
    labelName: String,
    featureNames: List[String]) extends TypedMetaGraphOp[Input, Output] with ModelMeta {
  val isClassification = true
  override val generatesProbability = true
  override val isHeavy = true
  @transient override lazy val inputs = new Input(featureNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "maxIter" -> maxIter,
    "labelName" -> labelName,
    "featureNames" -> featureNames)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._

    val rddArray = inputs.features.toArray.map(_.rdd)
    val featuresRDD = Model.toLinalgVector(rddArray, inputs.vertices.rdd)
    val labeledFeaturesDF = featuresRDD.sortedJoin(inputs.label.rdd).values.toDF("vector", "label")
    val labelIndexer = new ml.feature.StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel").fit(labeledFeaturesDF)
    val indexedFeaturesDF = labelIndexer.transform(labeledFeaturesDF)
    // Train a logictic regression model. The model sets the threshold to be 0.5 and 
    // the feature scaling to be true by default.
    val logisticRegression = new ml.classification.LogisticRegression()
      .setMaxIter(maxIter)
      .setTol(0)
      .setFeaturesCol("vector")
      .setLabelCol("indexedLabel")
      .setRawPredictionCol("rawClassification")
      .setPredictionCol("indexedClassification")
      .setProbabilityCol("probability")
    val logisticRegressionModel = logisticRegression.fit(indexedFeaturesDF)
    val (fMeasure, threshold) = getFMeasureAndThreshold(logisticRegressionModel)
    logisticRegressionModel.setThreshold(threshold)
    val prediction = logisticRegressionModel.transform(indexedFeaturesDF)
    // Convert indexed labels back to original labels.
    val labelConverter = new ml.feature.IndexToString()
      .setInputCol("indexedClassification")
      .setOutputCol("classification")
      .setLabels(labelIndexer.labels)
    val pipeline = new ml.Pipeline().setStages(Array(labelIndexer, logisticRegressionModel, labelConverter))
    val model = pipeline.fit(labeledFeaturesDF)
    val statistics = getStatistics(logisticRegressionModel, prediction, featureNames) + s"F score: $fMeasure"

    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      method = "Logistic regression",
      symbolicPath = file.symbolicName,
      labelName = Some(labelName),
      featureNames = featureNames,
      // The feature vectors are standardized by the model. A dummy scaler is used here.
      featureScaler = None,
      statistics = Some(statistics)))
  }
  // Helper method to find the best threshold.
  private def getFMeasureAndThreshold(
    rawModel: ml.classification.LogisticRegressionModel): (Double, Double) = {
    val binarySummary = rawModel.summary.asInstanceOf[ml.classification.BinaryLogisticRegressionSummary]
    val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(sql.functions.max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where(fMeasure("F-Measure") === maxFMeasure)
      .select("threshold").head().getDouble(0)
    (maxFMeasure, bestThreshold)
  }
  // Helper method to compute statistics where the training data is required.
  private def getStatistics(
    regModel: ml.classification.LogisticRegressionModel,
    predictions: sql.DataFrame,
    featureNames: List[String]): String = {
    val numFeatures = regModel.coefficients.size
    val numData = predictions.count.toInt
    val predictionAndLabels = predictions.select("rawClassification", "indexedLabel")
    val coefAndIntercept = regModel.coefficients.toArray ++ Array(regModel.intercept)
    val mcfaddenR2 = {
      val logLikFull = predictionAndLabels.map {
        row =>
          {
            val rawClassification = row.getAs[mllib.linalg.DenseVector](0)(1)
            val label = row.getDouble(1)
            rawClassification * label - Math.log(1 + Math.exp(rawClassification))
          }
      }.sum
      val logLikIntercept = {
        val labelArray = predictionAndLabels.map(_.getDouble(1)).collect
        val labelSum = labelArray.sum
        val nullProb = labelSum / numData
        val nullIntercept = Math.log(nullProb / (1 - nullProb))
        nullIntercept * labelSum - numData * Math.log(1 + Math.exp(nullIntercept))
      }
      1 - logLikFull / logLikIntercept
    }
    val zValues = {
      val vectors = predictions.map(_.getAs[mllib.linalg.Vector]("vector"))
      val flattenMatrix = vectors.map(_.toArray ++ Array(1.0)).reduce(_ ++ _)
      val matrix = new linalg.DenseMatrix(
        rows = numFeatures + 1,
        cols = numData,
        data = flattenMatrix)
      val probability = predictions.map(_.getAs[mllib.linalg.Vector]("probability"))
      val cost = probability.map(prob => prob(0) * prob(1)).collect
      val matrixCost = linalg.diag(linalg.DenseVector(cost))
      val covariance = linalg.inv((matrix * (matrixCost * matrix.t)))
      val coefStdErr = linalg.diag(covariance).map(Math.sqrt(_))
      val zValues = linalg.DenseVector(coefAndIntercept) :/ coefStdErr
      zValues.toArray
    }
    val tableEntry: Array[Array[String]] = Array(featureNames.toArray ++ Array("intercept"),
      coefAndIntercept.map(_.toFloat.toString), zValues.map(_.toFloat.toString)).transpose
    val table = model.Tabulator.format(Array(Array("names", "estimate", "Z-values")) ++
      tableEntry)
    s"coefficients:\n$table\npseudo R-squared: $mcfaddenR2, "
  }
}
