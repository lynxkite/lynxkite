package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api._
import org.scalatest.funsuite.AnyFunSuite

class LookupRegionTest extends AnyFunSuite with TestGraphOp {
  test("find timezones for the ExampleGraph") {
    val shapePath = getClass.getResource("/graph_operations/FindRegionTest/earth.shp").getPath
    val ex = ExampleGraph()().result
    val op = LookupRegion(shapePath, "TZID", ignoreUnsupportedShapes = false)
    val result = op(op.coordinates, ex.location).result
    assert(result.attribute.rdd.collect().toSet ==
      Set((1, "Europe/Budapest"), (2, "Asia/Kuala_Lumpur"), (3, "Australia/Sydney")))
  }
}
