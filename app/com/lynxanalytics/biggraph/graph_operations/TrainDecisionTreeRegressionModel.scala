// Trains a decision tree regression model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import org.apache.spark.ml
import org.apache.spark.sql

object TrainDecisionTreeRegressor extends OpFromJson {
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
  def fromJson(j: JsValue) = TrainDecisionTreeRegressor(
    (j \ "labelName").as[String],
    (j \ "featureNames").as[List[String]],
    (j \ "impurity").as[String],
    (j \ "maxBins").as[Int],
    (j \ "maxDepth").as[Int],
    (j \ "minInfoGain").as[Double],
    (j \ "minInstancesPerNode").as[Int],
    (j \ "seed").as[Int])
}

import TrainDecisionTreeRegressor._
case class TrainDecisionTreeRegressor(
    labelName: String,
    featureNames: List[String],
    impurity: String = "variance",
    maxBins: Int = 32,
    maxDepth: Int = 5,
    minInfoGain: Double = 0.0,
    minInstancesPerNode: Int = 1,
    seed: Int = 1234567) extends TypedMetaGraphOp[Input, Output] with ModelMeta {
  val isClassification = false
  val isBinary = false
  override val generatesProbability = true
  override val isHeavy = true
  @transient override lazy val inputs = new Input(featureNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "labelName" -> labelName,
    "featureNames" -> featureNames,
    "impurity" -> impurity,
    "maxBins" -> maxBins,
    "maxDepth" -> maxDepth,
    "minInfoGain" -> minInfoGain,
    "minInstancesPerNode" -> minInstancesPerNode,
    "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._

    val featuresRddArray = inputs.features.toArray.map(_.rdd)
    val labelDF = inputs.label.rdd.toDF("id", "label")
    val featuresDF = Model.toDF(sqlContext, inputs.vertices.rdd, featuresRddArray)
    val labeledFeaturesDF = featuresDF.join(labelDF, "id")
    assert(!labeledFeaturesDF.rdd.isEmpty, "Training is not possible with empty data set.")

    val decisionTreeRegressor = new ml.regression.DecisionTreeRegressor()
      .setImpurity(impurity)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setSeed(seed)
    val model = decisionTreeRegressor.fit(labeledFeaturesDF)
    val file = Model.newModelFile
    model.save(file.resolvedName)
    val treeDescription = (0 until featureNames.length).foldLeft { model.toDebugString } {
      (description, i) => description.replaceAll("feature " + i.toString, featureNames(i))
    }.replaceFirst("[(]uid=.*[)] ", "")
    val prediction = model.transform(labeledFeaturesDF)
    val evaluator = new ml.evaluation.RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(prediction).toString
    val statistics = ("Root mean squared error: " + rmse)
    println(statistics)
    output(o.model, Model(
      method = "Decision tree regression",
      symbolicPath = file.symbolicName,
      labelName = Some(labelName),
      featureNames = featureNames,
      statistics = Some(statistics)))
  }
}
