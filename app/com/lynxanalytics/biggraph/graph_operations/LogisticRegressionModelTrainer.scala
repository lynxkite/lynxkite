// Trains a logistic regression model. Currently, the class only supports binary classification.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.model._
import org.apache.spark.sql
import org.apache.spark.ml
import org.apache.spark.storage.StorageLevel

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

  // Z-values, the wald test of the logistic regression, can be calculated by dividing coefficient
  // values to coefficient standard errors. See more information at http://www.real-statistics.com/
  // logistic-regression/significance-testing-logistic-regression-coefficients.
  // This is placed here (instead of the class) to enable standalone testing
  private[graph_operations] def computeZValues(coefficientsAndIntercept: Array[Double], predictions: sql.DataFrame) = {
    val numFeatures = coefficientsAndIntercept.length - 1
    val labelSum = predictions.rdd.map(_.getAs[Double]("label")).sum
    if (labelSum == 0.0) {
      // In this extreme case, all coefficients equal to 0 and the intercept equals to -inf
      Array.fill(numFeatures)(0.0) :+ Double.NegativeInfinity
    } else if (labelSum == predictions.count.toInt) {
      // In this extreme case, all coefficients equal to 0 and the intercept equals to +inf
      Array.fill(numFeatures)(0.0) :+ Double.PositiveInfinity
    } else {
      val sampleSizeApprox = LoggedEnvironment.envOrElse("KITE_ZVALUE_SAMPLE", "100000").toInt
      val fraction = (sampleSizeApprox.toDouble / predictions.count()) min 1.0
      // Compute z-value on a small sample only (since the computation is not distributed). To make the sampling
      // repeatable, an arbitrary seed is specified.
      val sample = predictions.sample(withReplacement = false, fraction, seed = 23948720934L)
      sample.persist(StorageLevel.DISK_ONLY)
      val vectors = sample.rdd.map(_.getAs[ml.linalg.Vector]("features"))
      val probability = sample.rdd.map(_.getAs[ml.linalg.Vector]("probability"))
      // The constant field is added in order to get the statistics of the intercept.
      val flattenMatrix = vectors.flatMap(_.toArray :+ 1.0).collect()
      val matrix = new breeze.linalg.DenseMatrix(
        rows = numFeatures + 1,
        cols = sample.count().toInt,
        data = flattenMatrix)
      val cost = probability.map(prob => prob(0) * prob(1)).collect
      val matrixCost = breeze.linalg.diag(breeze.linalg.SparseVector(cost))
      // The covariance matrix is calculated by the equation: S = inverse((transpose(X)*V*X)). X is
      // the numData * (numFeature + 1) design matrix and V is the numData * numData diagonal matrix
      // whose diagonal elements are probability_i * (1 - probability_i).
      val covariance = breeze.linalg.inv(matrix * matrixCost * matrix.t)
      val coefficientsStdErr = breeze.linalg.diag(covariance).map(Math.sqrt)
      val zValues = breeze.linalg.DenseVector(coefficientsAndIntercept) :/ coefficientsStdErr
      zValues.toArray
    }
  }
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
    val labelDF = inputs.label.rdd.toDF("id", "label")
    val featuresDF = Model.toDF(sqlContext, inputs.vertices.rdd, rddArray)
    val labeledFeaturesDF = featuresDF.join(labelDF, "id")
    assert(!labeledFeaturesDF.rdd.isEmpty, "Training is not possible with empty data set.")

    // Train a logistic regression model. The model chooses a threshold with minimal
    // F-score. Feature scaling is set to be true by default.
    val logisticRegression = new ml.classification.LogisticRegression()
      .setMaxIter(maxIter)
      .setTol(0)
      .setRawPredictionCol("rawClassification")
      .setPredictionCol("classification")
      .setProbabilityCol("probability")
    val model = logisticRegression.fit(labeledFeaturesDF)
    val (fMeasure, threshold) = getFMeasureAndThreshold(model)
    // If the estimated probability of class label 1 is > threshold, then predict 1, else 0.
    model.setThreshold(threshold)
    val prediction = model.transform(labeledFeaturesDF)
    val coefficientsTable = getCoefficientsTable(model, prediction, featureNames)
    val mcfaddenR2 = getMcfaddenR2(prediction)
    val table = Tabulator.getTable(
      headers = Array("", ""),
      rowNames = Array("pseudo R-squared:", "threshold:", "F-score:"),
      columnData = Array(Array(mcfaddenR2, threshold, fMeasure))
    )
    val statistics = coefficientsTable + table
    // "%18s".format("threshold: ") +
    // f"$threshold%1.6f\n" + "%18s".format("F-score: ") + f"$fMeasure%1.6f\n"
    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      method = "Logistic regression",
      symbolicPath = file.symbolicName,
      labelName = Some(labelName),
      featureNames = featureNames,
      statistics = Some(statistics)))
  }
  // Helper method to find the best threshold.
  private def getFMeasureAndThreshold(
    model: ml.classification.LogisticRegressionModel): (Double, Double) = {
    val binarySummary = model.summary.asInstanceOf[ml.classification.BinaryLogisticRegressionSummary]
    val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(sql.functions.max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where(fMeasure("F-Measure") === maxFMeasure)
      .select("threshold").head().getDouble(0)
    (maxFMeasure, bestThreshold)
  }
  // Helper method to find the pseudo R-squared.
  private def getMcfaddenR2(
    predictions: sql.DataFrame): Double = {
    val numData = predictions.count.toInt
    val labelSum = predictions.rdd.map(_.getAs[Double]("label")).sum
    if (labelSum == 0.0 || labelSum == numData) {
      0.0 // In the extreme cases, all coefficients equal to 0 and R2 eqauls 0.
    } else {
      // The log likelihood of logistic regression is calculated according to the equation:
      // ll(rawPrediction) = Sigma(-log(1+e^(rawPrediction_i)) + y_i*rawPrediction_i).
      // For details, see http://www.stat.cmu.edu/~cshalizi/uADA/12/lectures/ch12.pdf (eq 12.10).
      val logLikCurrent = predictions.rdd.map {
        row =>
          {
            val rawClassification = row.getAs[ml.linalg.Vector]("rawClassification")(1)
            val label = row.getAs[Double]("label")
            rawClassification * label - Math.log(1 + Math.exp(rawClassification))
          }
      }.sum
      val logLikIntercept = {
        val nullProbability = labelSum / numData
        val nullIntercept = Math.log(nullProbability / (1 - nullProbability))
        nullIntercept * labelSum - numData * Math.log(1 + Math.exp(nullIntercept))
      }
      1 - logLikCurrent / logLikIntercept
    }
  }
  // Helper method to get the table of coefficients and Z-values.
  private def getCoefficientsTable(model: ml.classification.LogisticRegressionModel,
                                   predictions: sql.DataFrame,
                                   featureNames: List[String]): String = {
    val coefficientsAndIntercept = model.coefficients.toArray :+ model.intercept
    val zValues: Array[Double] = computeZValues(coefficientsAndIntercept, predictions)
    val table = Tabulator.getTable(
      headers = Array("names", "estimates", "Z-values"),
      rowNames = featureNames.toArray :+ "intercept",
      columnData = Array(coefficientsAndIntercept, zValues)
    )
    s"coefficients:\n$table\n"
  }
}
