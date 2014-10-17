package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.scalatest.FunSuite

import scala.util.Random
import scala.language.implicitConversions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

class FingerprintingTest extends FunSuite with TestGraphOp {
  test("test") {
    val graph = SmallTestGraph(Map(
      1 -> Seq(2, 3, 4), 5 -> Seq(6, 7, 8), 9 -> Seq(10, 11, 12), 13 -> Seq(14, 15, 16))).result
    val side = {
      val op = AddVertexAttribute(((1 to 8).map(_ -> "LEFT") ++ (9 to 16).map(_ -> "RIGHT")).toMap)
      op(op.vs, graph.vs).result.attr
    }
    val name = {
      val op = AddVertexAttribute(Map(
        1 -> "L1", 2 -> "A", 3 -> "B", 4 -> "C", 5 -> "L2", 6 -> "D", 7 -> "E", 8 -> "F",
        9 -> "R1", 10 -> "A", 11 -> "B", 12 -> "C", 13 -> "R2", 14 -> "D", 15 -> "E", 16 -> "F"))
      op(op.vs, graph.vs).result.attr
    }
    val weight = AddConstantAttribute.run(graph.es.asVertexSet, 1.0)
    val fingerprinting = {
      val op = Fingerprinting("LEFT", "RIGHT", 0, 1, 0)
      op(op.es, graph.es)(op.weight, weight)(op.side, side)(op.name, name).result
    }
    assert(fingerprinting.leftName.rdd.collect.toMap == Map(9L -> "L1", 13L -> "L2"))
    assert(fingerprinting.rightName.rdd.collect.toMap == Map(1L -> "R1", 5L -> "R2"))
  }
}
