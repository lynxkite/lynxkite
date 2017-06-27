// Trains a linear regression model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml

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
  val isBinary = false
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
    val labelDF = inputs.label.rdd.toDF("id", "label")
    val featuresDF = Model.toDF(sqlContext, inputs.vertices.rdd, rddArray)
    val inputDF = featuresDF.join(labelDF, "id")
    assert(!inputDF.rdd.isEmpty, "Training is not possible with empty data set.")

    val linearRegression = new ml.regression.LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
    // The following settings are according to the Spark MLLib deprecation codes. For example, see
    // org/apache/spark/mllib/regression/LinearRegression.scala (branch-2.0, line-106). And also
    // RidgeRegression.scala, Lasso.scala.
    method match {
      case "Linear regression" =>
        linearRegression.setElasticNetParam(0.0).setRegParam(0.0)
      case "Ridge regression" =>
        linearRegression.setElasticNetParam(0.0).setRegParam(0.01)
      case "Lasso" =>
        linearRegression.setElasticNetParam(1.0).setRegParam(0.01)
    }
    val model = linearRegression.fit(inputDF)
    val predictions = model.transform(inputDF)
    val statistics: String = getStatistics(model, predictions, featureNames)
    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      method = method,
      symbolicPath = file.symbolicName,
      labelName = Some(labelName),
      featureNames = featureNames,
      statistics = Some(statistics))
    )
  }
  // Helper method to compute more complex statistics.
  private def getStatistics(
    model: ml.regression.LinearRegressionModel,
    predictions: DataFrame,
    featureNames: List[String]): String = {
    val summary = model.summary
    val r2 = summary.r2
    val MAPE = Model.getMAPE(predictions.select("prediction", "label"))
    // Only compute the t-values for methods with unbiased solvers (when the elastic
    // net parameter equals to 0).
    val coefficientsTable = {
      val rowNames = featureNames.toArray :+ "intercept"
      val coefficientsAndIntercept = model.coefficients.toArray :+ model.intercept
      if (model.getElasticNetParam > 0.0) {
        Tabulator.getTable(
          headers = Array("names", "estimates"),
          rowNames = rowNames,
          columnData = Array(coefficientsAndIntercept))
      } else {
        Tabulator.getTable(
          headers = Array("names", "estimates", "T-values"),
          rowNames = rowNames,
          columnData = Array(coefficientsAndIntercept, summary.tValues))
      }
    }
    val table = Tabulator.getTable(
      headers = Array("", ""),
      rowNames = Array("R-squared:", "MAPE:"),
      columnData = Array(Array(r2, MAPE))
    )
    s"coefficients: \n$coefficientsTable\n$table"
  }
}
