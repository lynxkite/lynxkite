package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.SparkFreeEnvironment
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_operations
import com.lynxanalytics.lynxkite.graph_util
import com.lynxanalytics.lynxkite.graph_util.Scripting._
import com.lynxanalytics.lynxkite.controllers._

class HiddenOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Implicits._

  val category = Categories.HiddenOperations

  import com.lynxanalytics.lynxkite.controllers.OperationParams._

  register("Anchor", List(), List(), "anchor")(new DecoratorOperation(_) {
    params += Code("description", "Description", language = "plain_text")
    params += Param("icon", "Icon URL")
    params += ParametersParam("parameters", "Parameters")
    params += Choice("wizard", "Wizard", options = FEOption.noyes)
    if (params("wizard") == "yes") {
      params += Choice("in_progress", "In progress", options = FEOption.noyes)
      params += WizardStepsParam("steps", "Steps")
    }
  })

  register("Check cliques")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {
      params += Param("selected", "Segment IDs to check", defaultValue = "<All>")
      params += Choice("bothdir", "Edges required in both directions", options = FEOption.bools)
    }
    def enabled = project.hasVertexSet
    def apply() = {
      val selected =
        if (params("selected") == "<All>") None
        else Some(splitParam("selected").map(_.toLong).toSet)
      val op = graph_operations.CheckClique(selected, params("bothdir").toBoolean)
      val result = op(op.es, parent.edgeBundle)(op.belongsTo, seg.belongsTo).result
      parent.scalars("invalid_cliques") = result.invalid
    }
  })

  registerProjectCreatingOp(
    "Create enhanced example graph")(new ProjectOutputOperation(_) {
    def enabled = FEStatus.enabled
    def apply() = {
      val g = graph_operations.EnhancedExampleGraph()().result
      project.vertexSet = g.vertices
      project.edgeBundle = g.edges
      for ((name, attr) <- g.vertexAttributes) {
        project.newVertexAttribute(name, attr)
      }
      project.edgeAttributes = g.edgeAttributes.mapValues(_.entity)
    }
  })

  registerProjectCreatingOp("Use metagraph as graph")(new ProjectOutputOperation(_) {
    params +=
      Param("timestamp", "Current timestamp", defaultValue = graph_util.Timestamp.toString)
    def enabled =
      FEStatus.assert(user.isAdmin, "Requires administrator privileges")
    def apply() = {
      val t = params("timestamp")
      val mg = graph_operations.MetaGraph(t, Some(env)).result
      project.vertexSet = mg.vs
      project.newVertexAttribute("GUID", mg.vGUID)
      project.newVertexAttribute("kind", mg.vKind)
      project.newVertexAttribute("name", mg.vName)
      project.newVertexAttribute("progress", mg.vProgress)
      project.edgeBundle = mg.es
      project.newEdgeAttribute("kind", mg.eKind)
      project.newEdgeAttribute("name", mg.eName)
    }
  })

  for (i <- 1 to 10) {
    register(
      s"External computation $i",
      (1 to i).map(_.toString).toList,
      List("table"),
      "hat-cowboy")(new TableOutputOperation(_) {
      params += Param("snapshot_prefix", "Snapshot prefix")
      def enabled = FEStatus.enabled
      override def getOutputs() = {
        // Make sure the snapshot name depends on all the input GUIDs.
        val snapshotName =
          params("snapshot_prefix") +
            (1 to i).map(j => exportResultInput(j.toString).gUID.toString).mkString("-")
        val snapshot = DirectoryEntry.fromName(snapshotName).asSnapshotFrame
        makeOutput(snapshot.getState.table)
      }
    })
  }
}
