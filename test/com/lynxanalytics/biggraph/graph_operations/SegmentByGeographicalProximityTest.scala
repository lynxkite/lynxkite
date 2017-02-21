package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Shapefile
import org.scalatest.FunSuite

class SegmentByGeographicalProximityTest extends FunSuite with TestGraphOp {
  test("segment the ExampleGraph by geographical proximity") {
    val shapePath = getClass.getResource("/graph_operations/FindRegionTest/earth.shp").getPath
    val shapeFile = Shapefile(shapePath)
    val ex = ExampleGraph()().result
    val op = SegmentByGeographicalProximity(shapePath, 0.1, shapeFile.attrNames, onlyKnownFeatures = true)
    val result = op(op.coordinates, ex.location).result
    assert(result.segments.rdd.count == 418)
    // Test that an overlapping segment was created. The actual values are checked in the
    // corresponding frontend test SegmentByGEODataOperationTest.
    assert(result.belongsTo.rdd.values.map {
      case (Edge(vid, sid)) => (vid, sid)
    }.collect.toSet == Set((0, 144), (1, 172), (2, 325), (2, 305), (3, 204)))
  }
}
