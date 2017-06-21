package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_operations.{ DynamicValue, DoubleGT, VertexAttributeFilter }
import com.lynxanalytics.biggraph.spark_util.IDBuckets
import org.scalatest.FunSuite

import scala.reflect.runtime.universe._

class TypeTagToFormatTest extends FunSuite {

  def testTag[T: TypeTag](value: T): Unit = {
    val typeTag = implicitly[TypeTag[T]]
    val jsonRepr = TypeTagToFormat.typeTagToFormat(typeTag).writes(value)
    val readBack = TypeTagToFormat.typeTagToFormat(typeTag).reads(jsonRepr)
    assert(readBack.get == value)
  }

  test("TypeTag -> json conversions (simple types)") {
    testTag("hello")
    testTag(42L)
    testTag(42)
    testTag(3.1415)
  }

  test("TypeTag -> json conversions (options)") {
    testTag(Option(1.0))
    testTag[Option[Int]](None)
    testTag(Option(100L))
    testTag[Option[Option[String]]](Some(Some("embedded option")))
  }

  test("TypeTag -> json conversions (Edge)") {
    testTag(Edge(10L, 20L))
  }

  test("TypeTag -> json conversions (DynamicValue)") {
    val dv = DynamicValue("name", true, Some(1.2), Some(1.3), None)
    testTag(dv)
  }

  test("TypeTag -> json conversions (UIStatus)") {
    val uiStatus =
      UIStatus(
        Some(""),
        Some("sampled"),
        "svg",
        UIFilterStatus(Map(), Map()),
        "4",
        Some(false),
        Some(false),
        UIAxisOptions(Map(), Map()),
        "1",
        Map(),
        UIAnimation(false, "neutral", "0"),
        None,
        Some(UICenterRequest(1, List(), None)),
        Some(false),
        Some("42"))
    testTag(uiStatus)
  }

  test("TypeTag -> json conversions (JsonTraits)") {
    val filter = VertexAttributeFilter[Double](DoubleGT(2))
    testTag(filter)
  }

  test("TypeTag -> json conversions (Tuples)") {
    testTag((1, 2))
    testTag((3.14, "SomeString"))
  }

  test("TypeTag -> json conversions (Scala collections)") {
    val seqOfTuples = (1 to 10).zip(11 to 20).seq
    testTag[IndexedSeq[(Int, Int)]](seqOfTuples)

    testTag(seqOfTuples.toList)
    testTag(seqOfTuples.toSet)

    val mapOfIntString = Map(1 -> "one", 2 -> "two", 3 -> "three")
    testTag(mapOfIntString)

    val mapOfStringDouble = Map("one" -> 1.0, "twenty" -> 20.0, "minuspi" -> -3.1415)
    testTag(mapOfStringDouble)
  }

  test("TypeTag - IDBuckets") {
    val nullSampleBucket = new IDBuckets[(Int, Int)]
    nullSampleBucket.sample = null
    testTag(nullSampleBucket)
    nullSampleBucket.add(1L, (1, 1))
    testTag(nullSampleBucket)
    nullSampleBucket.add(1L, (1, 1))
    testTag(nullSampleBucket)

    val bucket = new IDBuckets(nullSampleBucket.counts)
    bucket.add(0L, (0, 1))
    testTag(bucket)
  }
}
