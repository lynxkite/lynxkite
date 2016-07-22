// Trains a linear regression model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.Vectors

object RegressionModelTrainer extends OpFromJson {
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
  def fromJson(j: JsValue) = RegressionModelTrainer(
    (j \ "method").as[String],
    (j \ "labelName").as[String],
    (j \ "featureNames").as[List[String]])
}
import RegressionModelTrainer._
case class RegressionModelTrainer(
    method: String,
    labelName: String,
    featureNames: List[String]) extends TypedMetaGraphOp[Input, Output] with ModelMeta {
  val isClassification = false
  override val isHeavy = true
  @transient override lazy val inputs = new Input(featureNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "method" -> method,
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
    val scaledDF = featuresRDD.sortedJoin(inputs.label.rdd).values.toDF("vector", "label")

    val linearRegression = new ml.regression.LinearRegression()
      .setFeaturesCol("vector")
      .setLabelCol("label")
      .setPredictionCol("prediction")
    // The following settings are according to the Spark MLLib deprecation codes. For example, see
    // org/apache/spark/mllib/regression/LinearRegression.scala (branch-2.0, line-106).
    method match {
      case "Linear regression" =>
        linearRegression.setElasticNetParam(0.0).setRegParam(0.0)
      case "Ridge regression" =>
        linearRegression.setElasticNetParam(0.0).setRegParam(0.01)
      case "Lasso" =>
        linearRegression.setElasticNetParam(1.0).setRegParam(0.01)
    }
    val model = linearRegression.fit(scaledDF)
    val predictions = model.transform(scaledDF)
    val statistics: String = getStatistics(model, predictions)
    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      method = method,
      symbolicPath = file.symbolicName,
      labelName = Some(labelName),
      featureNames = featureNames,
      // The features and labels are standardized by the model. A dummy scaler is used here.
      featureScaler = None,
      statistics = Some(statistics))
    )
  }

  // Helper method to compute statistics where the training data is required.
  private def getStatistics(
    model: ml.regression.LinearRegressionModel,
    predictions: DataFrame): String = {
    val summary = model.summary
    val r2 = summary.r2
    val MAPE = predictions.select("prediction", "label").map {
      row => {
        // Return an error of 100% if a zero division error occurs
        if (row.getDouble(1) == 0) {
          1.0
        } else {
          math.abs(row.getDouble(0) / row.getDouble(1) - 1.0)
        }
      }
    }.mean * 100
    // Only compute the t-values for methods with unbiased solvers (when the elastic 
    // net parameter equals to 0).
    if (model.getElasticNetParam > 0.0) {
      s"R-squared: $r2\nMAPE: $MAPE%"
    } else {
      val tValues = "(" + summary.tValues.mkString(", ") + ")"
      s"R-squared: $r2\nMAPE: $MAPE%\nT-values: $tValues"
    }
  }
}
