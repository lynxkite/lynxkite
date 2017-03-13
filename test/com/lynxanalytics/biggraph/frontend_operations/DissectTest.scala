package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.CommonProjectState
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DissectTest extends OperationsTestBase {
  test("Take segmentation as base project") {
    run("Create example graph")
    run("Segment by string attribute", Map("name" -> "gender", "attr" -> "gender"))
    val stateOfTheSegmentation = project.state.segmentations("gender").state
    run("Take segmentation as base project", Map("segmentation" -> "gender"))
    // The vertex_count_delta is updated after each operation so the project's state has now a
    // vertex_count_delta while the stateOfSegmentation does not.
    val projectStateWithoutVertexCountDelta = project.state.copy(
      scalarGUIDs = project.state.scalarGUIDs - "!vertex_count_delta")
    assert(projectStateWithoutVertexCountDelta == stateOfTheSegmentation)
  }

  test("Take edgebundle as vertex set") {
    run("Create enhanced example graph")
    val originalEdgeID = project.edgeBundle.idSet
    val originalEdgeAttributes = project.edgeAttributes
    run("Take edgebundle as vertex set")
    assert(project.vertexSet == originalEdgeID)
    assert(project.vertexAttributes == originalEdgeAttributes)
    assert(project.scalars.keys == Set("vertex_count", "!vertex_count_delta"))
    assert(project.scalars("vertex_count").value == 19)
    assert(project.edgeBundle == null)
    assert(project.segmentations == Seq())
  }
}
