// Trains a linear regression model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import org.apache.spark.ml.regression.LinearRegression
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
    (j \ "maxIter").as[Int],
    (j \ "elasticNetParam").as[Double],
    (j \ "regParam").as[Double],
    (j \ "labelName").as[String],
    (j \ "featureNames").as[List[String]])
}
import RegressionModelTrainer._
case class RegressionModelTrainer(
    maxIter: Int,
    elasticNetParam: Double,
    regParam: Double,
    labelName: String,
    featureNames: List[String]) extends TypedMetaGraphOp[Input, Output] with ModelMeta {
  val isClassification = false
  override val isHeavy = true
  @transient override lazy val inputs = new Input(featureNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "maxIter" -> maxIter,
    "elasticNetParam" -> elasticNetParam,
    "regParam" -> regParam,
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

    val linearRegrssion = new LinearRegression()
      .setMaxIter(maxIter)
      .setElasticNetParam(elasticNetParam)
      .setRegParam(regParam)
      .setFeaturesCol("vector")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val model = linearRegrssion.fit(scaledDF)
    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      method = "Linear regression",
      symbolicPath = file.symbolicName,
      labelName = Some(labelName),
      featureNames = featureNames,
      // The ML library doesn't use SGD for optimization so label scaler is unnecessary. 
      labelScaler = None,
      featureScaler = {
        val dummyVector = Vectors.dense(Array.fill(featureNames.size)(0.0))
        new StandardScalerModel(
          std = dummyVector, mean = dummyVector, withStd = false, withMean = false)
      })
    )
  }
}
