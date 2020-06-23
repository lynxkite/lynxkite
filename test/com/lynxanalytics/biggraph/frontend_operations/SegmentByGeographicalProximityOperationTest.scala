package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class SegmentByGeographicalProximityOperationTest extends OperationsTestBase {

  // Creates symbolic links for a Shapefile under the kite meta dir. This is needed because
  // the frontend expects the Shapefile in a specific directory.
  def linkShapeFile(name: String): String = {
    import java.io.File
    import java.nio.file.Files
    import java.nio.file.Paths
    def metaDir = new File(metaGraphManager.repositoryPath).getParent
    val shapeFilesDirPath = s"$metaDir/resources/shapefiles"
    Files.createDirectories(Paths.get(shapeFilesDirPath))
    for (ext <- Seq(".shp", ".shx", ".dbf")) {
      val oldPath = getClass.getResource("/graph_operations/FindRegionTest/" + name + ext).getPath
      val newPath = s"$shapeFilesDirPath/$name$ext"
      Files.createSymbolicLink(Paths.get(newPath), Paths.get(oldPath))
    }
    shapeFilesDirPath + "/" + name + ".shp"
  }

  test("Segment by geographical proximity") {
    val shapePath = linkShapeFile("earth")
    val base = box("Create example graph")
      .box("Segment by geographical proximity", Map(
        "name" -> "timezones",
        "position" -> "location",
        "shapefile" -> shapePath,
        "distance" -> "0.1",
        "ignoreUnsupportedShapes" -> "false"))
    val seg = base.project.segmentation("timezones")
    // Assert that the TZID segment attribute is created.
    assert(seg.vertexAttributes("TZID").runtimeSafeCast[String].rdd.count == 418)
    val project = base.box(
      "Aggregate from segmentation",
      Map("prefix" -> "timezones", "aggregate_TZID" -> "set", "apply_to_graph" -> ".timezones"))
      .project
    val tzids = project.vertexAttributes("timezones_TZID_set").runtimeSafeCast[Set[String]]
    assert(tzids.rdd.collect.toSet == Set(
      (0, Set("America/New_York")),
      (1, Set("Europe/Budapest")),
      (2, Set("Asia/Kuala_Lumpur", "Asia/Singapore")),
      (3, Set("Australia/Sydney"))))
  }
}
