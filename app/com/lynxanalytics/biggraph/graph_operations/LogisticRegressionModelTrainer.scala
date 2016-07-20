// Trains a logistic regression model. Currently, the class only supports binary 
// classification.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.Vectors

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
    val scaledDF = featuresRDD.sortedJoin(inputs.label.rdd).values.toDF("vector", "label")
    // Train a logictic regression model. The model sets the threshold to be 0.5 and 
    // the feature scaling to be true by default.
    val logisticRegression = new LogisticRegression()
      .setMaxIter(maxIter)
      .setTol(0)
      .setFeaturesCol("vector")
      .setLabelCol("label")
      .setPredictionCol("classification")
      .setProbabilityCol("probability")

    val model = logisticRegression.fit(scaledDF)
    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      method = "Logistic regression",
      symbolicPath = file.symbolicName,
      labelName = Some(labelName),
      featureNames = featureNames,
      // The feature vectors are standardized by the model. A dummy scaler is used here.
      featureScaler = {
        val dummyVector = Vectors.dense(Array.fill(featureNames.size)(0.0))
        new StandardScalerModel(
          std = dummyVector, mean = dummyVector, withStd = false, withMean = false)
      },
      statistics = None))
  }
}
