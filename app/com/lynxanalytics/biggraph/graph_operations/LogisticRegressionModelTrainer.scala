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
import breeze.{ linalg => linalgBreeze }

object LogisticRegressionModelTrainer extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val label = vertexAttribute[Double](vertices)
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

    // Train a logictic regression model. The model sets the threshold to be 0.5 and 
    // the feature scaling to be true by default.
    val logisticRegression = new ml.classification.LogisticRegression()
      .setMaxIter(maxIter)
      .setTol(0)
      .setFeaturesCol("vector")
      .setLabelCol("label")
      .setRawPredictionCol("rawClassification")
      .setPredictionCol("indexedClassification")
      .setProbabilityCol("probability")
    val model = logisticRegression.fit(labeledFeaturesDF)
    val (fMeasure, threshold) = getFMeasureAndThreshold(model)
    model.setThreshold(threshold)
    val prediction = model.transform(labeledFeaturesDF)
    val statistics = getStatistics(model, prediction, featureNames) +
      s"threshold: $threshold, F-score: $fMeasure\n"

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
    val coefficientsAndIntercept = regModel.coefficients.toArray :+ regModel.intercept
    val mcfaddenR2: Double = {
      val logLikCurrent = predictions.map {
        row =>
          {
            val rawClassification = row.getAs[mllib.linalg.DenseVector]("rawClassification")(1)
            val label = row.getAs[Double]("label")
            rawClassification * label - Math.log(1 + Math.exp(rawClassification))
          }
      }.sum
      val logLikIntercept = {
        val labelSum = predictions.map(_.getAs[Double]("label")).sum
        val nullProbability = labelSum / numData
        val nullIntercept = Math.log(nullProbability / (1 - nullProbability))
        nullIntercept * labelSum - numData * Math.log(1 + Math.exp(nullIntercept))
      }
      1 - logLikCurrent / logLikIntercept
    }
    val zValues: Array[Double] = {
      val vectors = predictions.map(_.getAs[mllib.linalg.DenseVector]("vector"))
      val probability = predictions.map(_.getAs[mllib.linalg.DenseVector]("probability"))
      // The constant field is added in order to get the statistics for the intercept
      val flattenMatrix = vectors.map(_.toArray :+ 1.0).reduce(_ ++ _)
      val matrix = new linalgBreeze.DenseMatrix(
        rows = numFeatures + 1,
        cols = numData,
        data = flattenMatrix)
      val cost = probability.map(prob => prob(0) * prob(1)).collect
      val matrixCost = linalgBreeze.diag(linalgBreeze.DenseVector(cost))
      val covariance = linalgBreeze.inv((matrix * (matrixCost * matrix.t)))
      val coefficientsStdErr = linalgBreeze.diag(covariance).map(Math.sqrt(_))
      val zValues = linalgBreeze.DenseVector(coefficientsAndIntercept) :/ coefficientsStdErr
      zValues.toArray
    }
    val table = model.Tabulator.getTable(
      headers = Array("names", "estimates", "Z-values"),
      rowNames = featureNames.toArray :+ "intercept",
      columnData = Array(coefficientsAndIntercept, zValues))
    s"coefficients:\n$table\npseudo R-squared: $mcfaddenR2\n"
  }
}
