package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

class LookupRegionTest extends FunSuite with TestGraphOp {
  test("find timezones for the ExampleGraph") {
    val ex = ExampleGraph()().result
    val op = LookupRegion()
    val result = op(op.coordinates, ex.location).result
    assert(result.region.rdd.collect().toSet ==
      Set((0, "America/New_York"), (1, "Europe/Budapest"), (2, "Asia/Jakarta"), (3, "uninhabited")))
  }
}
