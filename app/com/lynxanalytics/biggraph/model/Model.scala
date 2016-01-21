package com.lynxanalytics.biggraph.model

import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.mllib
import org.apache.spark.rdd.RDD

trait ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double]
}

case class GeneralizedLinearModelImpl(m: mllib.regression.GeneralizedLinearModel) extends ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double] = { m.predict(data) }
}
case class ClassificationModelImpl(m: mllib.classification.ClassificationModel) extends ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double] = { m.predict(data) }
}
case class DecisionTreeModelImpl(m: mllib.tree.model.DecisionTreeModel) extends ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double] = { m.predict(data) }
}
case class RandomForestModelImpl(m: mllib.tree.model.RandomForestModel) extends ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double] = { m.predict(data) }
}
case class GradientBoostedTreesModelImpl(m: mllib.tree.model.GradientBoostedTreesModel) extends ModelImplementation {
  def predict(data: RDD[mllib.linalg.Vector]): RDD[Double] = { m.predict(data) }
}

abstract case class Model(modelType: String, path: String) {

  def model(implicit rc: RuntimeContext): ModelImplementation = {
    val sc = rc.sparkContext
    modelType match {
      case "Linear regression" =>
        GeneralizedLinearModelImpl(mllib.regression.LinearRegressionModel.load(sc, path))
      case "Logistic regression" =>
        GeneralizedLinearModelImpl(mllib.classification.LogisticRegressionModel.load(sc, path))
      case "Naive Bayes" =>
        ClassificationModelImpl(mllib.classification.NaiveBayesModel.load(sc, path))
      case "Decision tree" =>
        DecisionTreeModelImpl(mllib.tree.model.DecisionTreeModel.load(sc, path))
      case "Random forest" =>
        RandomForestModelImpl(mllib.tree.model.RandomForestModel.load(sc, path))
      case "Gradient-boosted trees" =>
        GradientBoostedTreesModelImpl(mllib.tree.model.GradientBoostedTreesModel.load(sc, path))
    }
  }

  def predict(data: RDD[mllib.linalg.Vector])(implicit rc: RuntimeContext): RDD[Double] = {
    model.predict(data)
  }
}
