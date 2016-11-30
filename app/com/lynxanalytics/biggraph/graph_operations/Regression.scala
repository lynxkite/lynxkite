// Trains a machine learning model and uses it to generate predictions of an attribute.
//
// MLlib can use native linear algebra packages when properly configured.
// See http://spark.apache.org/docs/latest/mllib-guide.html#dependencies.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.ml
import com.lynxanalytics.biggraph.model._

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
    implicit val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._

    val rddArray = inputs.features.toArray.map(_.rdd)
    val labelDF = inputs.label.rdd.toDF("id", "label")
    val featuresDF = Model.toDF(inputs.vertices.rdd, rddArray)
    val trainingDF = featuresDF.join(labelDF, "id")

    val predictionDF = method match {
      case "Linear regression" =>
        val model = new ml.regression.LinearRegression().setFitIntercept(true)
        val trained = model.fit(trainingDF)
        checkLinearModel(trained)
        trained.transform(featuresDF)
      case "Ridge regression" =>
        val model = new ml.regression.LinearRegression()
          .setFitIntercept(true)
          .setRegParam(0.01)
          .setElasticNetParam(0.0)
        val trained = model.fit(trainingDF)
        checkLinearModel(trained)
        trained.transform(featuresDF)
      case "Lasso" =>
        val model = new ml.regression.LinearRegression()
          .setFitIntercept(true)
          .setRegParam(0.01)
          .setElasticNetParam(1.0)
        val trained = model.fit(trainingDF)
        checkLinearModel(trained)
        trained.transform(featuresDF)
      case "Logistic regression" =>
        val model = new ml.classification.LogisticRegression
        val trained = model.fit(trainingDF)
        trained.transform(featuresDF)
      case "Naive Bayes" =>
        val model = new ml.classification.NaiveBayes
        val trained = model.fit(trainingDF)
        trained.transform(featuresDF)
      case "Decision tree" =>
        val model = new ml.regression.DecisionTreeRegressor
        val trained = model.fit(trainingDF)
        trained.transform(featuresDF)
      case "Random forest" =>
        val model = new ml.regression.RandomForestRegressor
        val trained = model.fit(trainingDF)
        trained.transform(featuresDF)
      case "Gradient-boosted trees" =>
        val model = new ml.regression.GBTRegressor
        val trained = model.fit(trainingDF)
        trained.transform(featuresDF)
    }
    val prediction = predictionDF.select("id", "prediction").as[(ID, Double)].rdd
    output(o.prediction, prediction.sortUnique(inputs.vertices.rdd.partitioner.get))
  }

  def checkLinearModel(model: ml.regression.LinearRegressionModel): Unit = {
    // A linear model with at least one NaN parameter will always predict NaN.
    for (w <- model.coefficients.toArray :+ model.intercept) {
      assert(!w.isNaN, "Failed to train a valid regression model.")
    }
  }
}
