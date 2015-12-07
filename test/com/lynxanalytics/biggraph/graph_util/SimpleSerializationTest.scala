package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.graph_api.Edge
import org.scalatest.FunSuite
import scala.reflect.runtime.universe._

class SimpleSerializationTest extends FunSuite {
  def serde[T](values: Seq[T], s: SimpleSerializer[T], d: SimpleDeserializer[T]) = {
    val numbered = values.zipWithIndex.map { case (v, k) => k.toLong -> v }
    val serialized = s.mapper(numbered.iterator).toSeq
    val deserialized = serialized.map { case (k, v) => k -> d.mapper(v) }.toList
    assert(deserialized == numbered)
  }

  test("basics") {
    def withTyped[T: TypeTag](values: Seq[T], expectedType: String) = {
      val s = SimpleSerializer.forType(typeTag[T])
      assert(s.name == expectedType)
      val d = SimpleDeserializer.forName[T](s.name)
      serde(values, s, d)
    }

    withTyped(Seq(1, 2, 3, 4), "kryo")
    withTyped(Seq((), ()), "unit")
    withTyped(Seq("alma", "beka"), "string")
    withTyped(Seq(5.5, 7.25), "double")
    withTyped(Seq(Edge(6L, 8L), Edge(7L, 9L)), "edge")
  }

  test("legacy") {
    def withKryo[T: TypeTag](values: Seq[T]) = {
      val s = SimpleSerializer.kryoSerializer.asInstanceOf[SimpleSerializer[T]]
      val d = SimpleDeserializer.forName[T]("kryo")
      serde(values, s, d)
    }

    withKryo(Seq(1, 2, 3, 4))
    // The Unit loaded back from Kryo is not equal to the original. (Similarly to SI-6935.)
    // withKryo(Seq((), ()))
    withKryo(Seq("alma", "beka"))
    withKryo(Seq(5.5, 7.25))
    withKryo(Seq(Edge(6L, 8L), Edge(7L, 9L)))
  }
}
