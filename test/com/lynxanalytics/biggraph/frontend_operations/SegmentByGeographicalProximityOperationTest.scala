package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

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
    run("Example Graph")
    val shapePath = linkShapeFile("earth")
    run("Segment by geographical proximity", Map(
      "name" -> "timezones",
      "position" -> "location",
      "shapefile" -> shapePath,
      "distance" -> "0.1",
      "onlyKnownFeatures" -> "true"))
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
