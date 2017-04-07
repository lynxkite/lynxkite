package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.CommonProjectState
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DissectTest extends OperationsTestBase {
  test("Take segmentation as base project") {
    val base = box("Create example graph")
      .box("Segment by string attribute", Map("name" -> "gender", "attr" -> "gender"))
    val stateOfTheSegmentation = base.project.state.segmentations("gender").state
    val project = base.box("Take segmentation as base project", Map("apply_to_project" -> "|gender")).project
    // The vertex_count_delta is updated after each operation so the project's state has now a
    // vertex_count_delta while the stateOfSegmentation does not.
    val projectStateWithoutVertexCountDelta = project.state.copy(
      scalarGUIDs = project.state.scalarGUIDs - "!vertex_count_delta")
    assert(projectStateWithoutVertexCountDelta == stateOfTheSegmentation)
  }

  test("Take edges as vertices") {
    val base = box("Create enhanced example graph")
    val originalEdgeID = base.project.edgeBundle.idSet
    val originalEdgeAttributes = base.project.edgeAttributes
    val originalVertexAttributes = base.project.vertexAttributes
    val project = base.box("Take edges as vertices").project
    assert(project.vertexSet == originalEdgeID)
    assert(project.vertexAttributes.keySet
      == originalEdgeAttributes.keySet.map("edge_" + _)
      ++ originalVertexAttributes.keySet.flatMap { k => Set("src_" + k, "dst_" + k) })
    for ((name, attr) <- originalEdgeAttributes) {
      assert(project.vertexAttributes("edge_" + name) == attr)
    }
    assert(project.scalars.keys == Set("vertex_count", "!vertex_count_delta"))
    assert(project.scalars("vertex_count").value == 19)
    assert(project.edgeBundle == null)
    assert(project.segmentations == Seq())
  }
}
