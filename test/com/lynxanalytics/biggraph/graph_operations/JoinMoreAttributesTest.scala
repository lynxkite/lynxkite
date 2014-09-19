package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

import org.apache.spark.SparkContext.rddToPairRDDFunctions

class JoinMoreAttributesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = JoinMoreAttributes[String](2, "default")
    val res = op(op.vs, g.vertices)(op.attrs, Seq(g.gender.entity, g.name.entity)).result.attr.rdd

    assert(res.collect.toList.map(_._2.toList) ==
      List(List("Male", "Adam"), List("Female", "Eve"), List("Male", "Bob"), List("Male", "Isolated Joe")))
  }
}