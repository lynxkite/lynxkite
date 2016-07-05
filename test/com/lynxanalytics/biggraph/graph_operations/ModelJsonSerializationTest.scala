package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.model._
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.DenseVector

class ModelJsonSerializationTest extends ModelTestBase {
  def createStandardScalerModel(
    std: List[Double],
    mean: List[Double],
    withStd: Boolean,
    withMean: Boolean): StandardScalerModel = {
    new StandardScalerModel(
      new DenseVector(std.toArray),
      new DenseVector(mean.toArray),
      withStd,
      withMean)
  }

  test("Model json serialization works") {
    val m1 = Model(
      "one",
      "two",
      Some("three"),
      List[String]("four", "five"),
      Some(createStandardScalerModel(List(100.0, 20.0), List(11.0, 22, 0), false, true)),
      createStandardScalerModel(List(1.0, 2.0), List(10.0, 20, 0), true, true))
    val out = m1.toJson
    val m2 = Model.fromJson(out)
    assert(m2 == m1)
  }

  test("Model json serialization works when labelScaler is None") {
    val m1 = Model(
      "one",
      "two",
      Some("three"),
      List[String]("four", "five"),
      None,
      createStandardScalerModel(List(1.0, 2.0), List(10.0, 20, 0), true, true))
    val out = m1.toJson
    val m2 = Model.fromJson(out)
    assert(m2 == m1)
  }

  test("Model json serialization works when labelScaler and labelName are None") {
    val m1 = Model(
      "one",
      "two",
      None,
      List[String]("four", "five"),
      None,
      createStandardScalerModel(List(1.0, 2.0), List(10.0, 20, 0), true, true))
    val out = m1.toJson
    val m2 = Model.fromJson(out)
    assert(m2 == m1)
  }
}
