package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import scala.collection.mutable

import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.graph_api._

class FindMaxCliquesTest extends FunSuite with TestGraphOperation {
  test("triangle") {
    val helper = cleanHelper
    val (sgv, sge) = helper.smallGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1)))
    val fmcOut = helper.apply(FindMaxCliques(3), Map('vsIn -> sgv, 'esIn -> sge))
    assert(helper.localData(fmcOut.vertexSets('cliques)).size == 1)
  }
}
