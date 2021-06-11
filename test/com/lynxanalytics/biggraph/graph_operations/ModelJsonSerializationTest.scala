package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
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
      Some(SerializableType.double),
      Some(Map(1.0 -> "a")),
      List[String]("four", "five"),
      Some(List(SerializableType.double)),
      Some(Map(0 -> Map("1" -> 2.0))),
      None,
    )
    val out = m1.toJson
    val m2 = Model.fromJson(out)
    assert(m2 == m1)
  }

  test("Model json serialization works when labelScaler is None") {
    val m1 = Model(
      "one",
      "two",
      Some("three"),
      Some(SerializableType.double),
      Some(Map(1.0 -> "a")),
      List[String]("four", "five"),
      Some(List(SerializableType.double)),
      Some(Map(0 -> Map("1" -> 2.0))),
      None,
    )
    val out = m1.toJson
    val m2 = Model.fromJson(out)
    assert(m2 == m1)
  }

  test("Model json serialization works when labelScaler and labelName are None") {
    val m1 = Model(
      "one",
      "two",
      None,
      Some(SerializableType.double),
      Some(Map(1.0 -> "a")),
      List[String]("four", "five"),
      Some(List(SerializableType.double)),
      Some(Map(0 -> Map("1" -> 2.0))),
      Some(""),
    )
    val out = m1.toJson
    val m2 = Model.fromJson(out)
    assert(m2 == m1)
  }

  test("Model json serialization works when labelType is None") {
    val m1 = Model(
      "one",
      "two",
      Some("three"),
      None,
      Some(Map(1.0 -> "a")),
      List[String]("four", "five"),
      Some(List(SerializableType.double)),
      Some(Map(0 -> Map("1" -> 2.0))),
      None)
    val out = m1.toJson
    val m2 = Model.fromJson(out)
    assert(m2 == m1)
  }
}
