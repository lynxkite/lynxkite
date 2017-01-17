package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.frontend_operations.OperationsTestBase
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

class LookupRegionTest extends OperationsTestBase {
  test("find timezones for the ExampleGraph") {
    run("Example Graph")
    run(
      "Create segmentation from SQL",
      Map("name" -> "latlon", "sql" ->
        """
          |select
          |  double(substring(split(string(location),",")[0], 2, 9)) as lat,
          |  double(substring(split(string(location),",")[1], 0, 8)) as lon
          |from vertices
        """.stripMargin))
    val shapefile = getClass.getResource("/graph_operations/FindRegionTest/earth.shp")
    val seg = project.segmentation("latlon")
    run("Lookup region", Map(
      "latitude" -> "lat", "longitude" -> "lon", "output" -> "timezone",
      "attribute" -> "TZID", "shapefile" -> shapefile.getPath), seg)
    assert(seg.vertexAttributes("timezone").rdd.values.collect().toSet ==
      Set("America/New_York", "Europe/Budapest", "Asia/Jakarta", "uninhabited"))
  }
}
