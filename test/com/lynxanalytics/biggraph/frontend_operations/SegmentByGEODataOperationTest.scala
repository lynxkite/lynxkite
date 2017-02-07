package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SegmentByGEODataOperationTest extends OperationsTestBase {
  test("Segment by GEO data") {
    run("Example Graph")
    val shapePath = getClass.getResource("/graph_operations/FindRegionTest/earth.shp").getPath
    run("Segment by GEO data", Map(
      "name" -> "timezones",
      "position" -> "location",
      "shapefile" -> shapePath,
      "distance" -> "0.1"))
    val seg = project.segmentation("timezones")
    // Assert that the TZID segment attribute is created.
    assert(seg.vertexAttributes("TZID").runtimeSafeCast[String].rdd.count == 418)
    run("Aggregate from segmentation",
      Map("prefix" -> "timezones", "aggregate-TZID" -> "vector"),
      on = seg)
    val tzids = project.vertexAttributes("timezones_TZID_vector").runtimeSafeCast[Vector[String]]
    assert(tzids.rdd.collect.toSet == Set(
      (0, Vector("America/New_York")),
      (1, Vector("Europe/Budapest")),
      (2, Vector("Asia/Singapore", "Asia/Kuala_Lumpur")),
      (3, Vector("Australia/Sydney"))))
  }
}
