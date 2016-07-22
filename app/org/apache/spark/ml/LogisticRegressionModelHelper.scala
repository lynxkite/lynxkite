package org.apache.spark.ml

import org.apache.spark.mllib
import org.apache.spark.ml.util._

object LogisticRegressionModelHelper {
  def nullModel(model: classification.LogisticRegressionModel): classification.LogisticRegressionModel = {
    val uid = Identifiable.randomUID("logreg")
    val coefficients = mllib.linalg.Vectors.zeros(model.coefficients.size)
    new classification.LogisticRegressionModel(uid, coefficients, model.intercept)
  }
}
