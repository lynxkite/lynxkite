// Trains a machine learning model and uses it to generate predictions of an attribute.
//
// MLlib can use native linear algebra packages when properly configured.
// See http://spark.apache.org/docs/latest/mllib-guide.html#dependencies.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.mllib
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

    val (predictions, vectors) = method match {
      case "Linear regression" =>
        val p = getParams(forSGD = true)
        val model = new mllib.regression.LinearRegressionWithSGD().setIntercept(true).run(p.labeledPoints.get)
        Model.checkLinearModel(model)
        (Model.scaleBack(model.predict(p.features.values), p.labelScaler.get), p.features)
      case "Ridge regression" =>
        val p = getParams(forSGD = true)
        val model = new mllib.regression.RidgeRegressionWithSGD().setIntercept(true).run(p.labeledPoints.get)
        Model.checkLinearModel(model)
        (Model.scaleBack(model.predict(p.features.values), p.labelScaler.get), p.features)
      case "Lasso" =>
        val p = getParams(forSGD = true)
        val model = new mllib.regression.LassoWithSGD().setIntercept(true).run(p.labeledPoints.get)
        Model.checkLinearModel(model)
        (Model.scaleBack(model.predict(p.features.values), p.labelScaler.get), p.features)
      case "Logistic regression" =>
        val p = getParams(forSGD = false)
        val model = new mllib.classification.LogisticRegressionWithLBFGS()
          .setIntercept(true).setNumClasses(10).run(p.labeledPoints.get)
        Model.checkLinearModel(model)
        (model.predict(p.features.values), p.features)
      case "Naive Bayes" =>
        val p = getParams(forSGD = false)
        val model = mllib.classification.NaiveBayes.train(p.labeledPoints.get)
        (model.predict(p.features.values), p.features)
      case "Decision tree" =>
        val p = getParams(forSGD = false)
        val model = mllib.tree.DecisionTree.trainRegressor(
          input = p.labeledPoints.get,
          categoricalFeaturesInfo = Map[Int, Int](), // All continuous.
          impurity = "variance", // Options: gini, entropy, variance as of Spark 1.6.0.
          maxDepth = 5,
          maxBins = 32)
        (model.predict(p.features.values), p.features)
      case "Random forest" =>
        val p = getParams(forSGD = false)
        val model = mllib.tree.RandomForest.trainRegressor(
          input = p.labeledPoints.get,
          categoricalFeaturesInfo = Map[Int, Int](), // All continuous.
          numTrees = 10,
          featureSubsetStrategy = "onethird",
          impurity = "variance", // Options: gini, entropy, variance as of Spark 1.6.0.
          maxDepth = 4,
          maxBins = 100,
          seed = 0)
        (model.predict(p.features.values), p.features)
      case "Gradient-boosted trees" =>
        val p = getParams(forSGD = false)
        val boostingStrategy = mllib.tree.configuration.BoostingStrategy.defaultParams("Regression")
        boostingStrategy.numIterations = 10
        boostingStrategy.treeStrategy.maxDepth = 5
        boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]() // All continuous.
        val model = mllib.tree.GradientBoostedTrees.train(p.labeledPoints.get, boostingStrategy)
        (model.predict(p.features.values), p.features)
    }
    val ids = vectors.keys // We just put back the keys with a zip.
    output(
      o.prediction,
      ids.zip(predictions).filter(!_._2.isNaN).asUniqueSortedRDD(vectors.partitioner.get))
  }

  // Creates the input for training and evaluation.
  private def getParams(
    forSGD: Boolean // Whether the data should be prepared for an SGD method.
    )(implicit id: DataSet): ScaledParams = {
    new Scaler(forSGD).scale(
      inputs.label.rdd,
      inputs.features.toArray.map { v => v.rdd },
      inputs.vertices.rdd)
  }
}
