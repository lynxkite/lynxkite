package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._

class ManageProjectOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Implicits._

  val category = Categories.ManageProjectOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Copy edge attribute")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Old name", options = project.edgeAttrList),
      Param("destination", "New name"))
    def enabled = FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    override def summary = {
      val from = params("name")
      val to = params("destination")
      s"Copy edge attribute $from to $to"
    }
    def apply() = {
      project.newEdgeAttribute(
        params("destination"), project.edgeAttributes(params("name")),
        project.viewer.getEdgeAttributeNote(params("name")))
    }
  })

  register("Copy scalar")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Old name", options = project.scalarList),
      Param("destination", "New name"))
    def enabled = FEStatus.assert(project.scalarList.nonEmpty, "No scalars")
    override def summary = {
      val from = params("name")
      val to = params("destination")
      s"Copy scalar $from to $to"
    }
    def apply() = {
      project.newScalar(
        params("destination"), project.scalars(params("name")),
        project.viewer.getScalarNote(params("name")))
    }
  })

  register("Copy segmentation")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Old name", options = project.segmentationList),
      Param("destination", "New name"))
    def enabled = FEStatus.assert(project.segmentationList.nonEmpty, "No segmentations")
    override def summary = {
      val from = params("name")
      val to = params("destination")
      s"Copy segmentation $from to $to"
    }
    def apply() = {
      val from = project.existingSegmentation(params("name"))
      val to = project.segmentation(params("destination"))
      to.segmentationState = from.segmentationState
    }
  })

  register("Copy vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Old name", options = project.vertexAttrList),
      Param("destination", "New name"))
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    override def summary = {
      val from = params("name")
      val to = params("destination")
      s"Copy vertex attribute $from to $to"
    }
    def apply() = {
      assert(params("destination").nonEmpty, "Please set the new attribute name.")
      project.newVertexAttribute(
        params("destination"), project.vertexAttributes(params("name")),
        project.viewer.getVertexAttributeNote(params("name")))
    }
  })

  register("Discard edge attributes")(new ProjectTransformation(_) {
    params += Choice("name", "Name", options = project.edgeAttrList, multipleChoice = true)
    def enabled = FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    override def summary = {
      val names = params("name").replace(",", ", ")
      s"Discard edge attributes: $names"
    }
    def apply() = {
      for (param <- splitParam("name")) {
        project.deleteEdgeAttribute(param)
      }
    }
  })

  register("Discard scalars")(new ProjectTransformation(_) {
    params += Choice("name", "Name", options = project.scalarList, multipleChoice = true)
    def enabled = FEStatus.assert(project.scalarList.nonEmpty, "No scalars")
    override def summary = {
      val names = params("name").replace(",", ", ")
      s"Discard scalars: $names"
    }
    def apply() = {
      for (param <- splitParam("name")) {
        project.deleteScalar(param)
      }
    }
  })

  register("Discard segmentation")(new ProjectTransformation(_) {
    params += Choice("name", "Name", options = project.segmentationList)
    def enabled = FEStatus.assert(project.segmentationList.nonEmpty, "No segmentations")
    override def summary = {
      val name = params("name")
      s"Discard segmentation: $name"
    }
    def apply() = {
      project.deleteSegmentation(params("name"))
    }
  })

  register("Rename edge attribute")(new ProjectTransformation(_) {
    params ++= List(
      Choice("before", "Old name", options = project.edgeAttrList),
      Param("after", "New name"))
    def enabled = FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    override def summary = {
      val from = params("before")
      val to = params("after")
      s"Rename edge attribute $from to $to"
    }
    def apply() = {
      project.edgeAttributes(params("after")) = project.edgeAttributes(params("before"))
      project.edgeAttributes(params("before")) = null
    }
  })

  register("Rename scalar")(new ProjectTransformation(_) {
    params ++= List(
      Choice("before", "Old name", options = project.scalarList),
      Param("after", "New name"))
    def enabled = FEStatus.assert(project.scalarList.nonEmpty, "No scalars")
    override def summary = {
      val from = params("before")
      val to = params("after")
      s"Rename scalar $from to $to"
    }
    def apply() = {
      project.scalars(params("after")) = project.scalars(params("before"))
      project.scalars(params("before")) = null
    }
  })

  register("Rename segmentation")(new ProjectTransformation(_) {
    params ++= List(
      Choice("before", "Old name", options = project.segmentationList),
      Param("after", "New name"))
    def enabled = FEStatus.assert(project.segmentationList.nonEmpty, "No segmentations")
    override def summary = {
      val from = params("before")
      val to = params("after")
      s"Rename segmentation $from to $to"
    }
    def apply() = {
      project.segmentation(params("after")).segmentationState =
        project.existingSegmentation(params("before")).segmentationState
      project.deleteSegmentation(params("before"))
    }
  })

  register("Rename or discard vertex attributes")(new ProjectTransformation(_) {
    params ++= project.vertexAttrList.map {
      attr => Param(s"change_${attr.id}", attr.id, defaultValue = attr.id)
    }
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    val attrParams = params.toMap.collect {
      case (before, after) if before.slice(0, 7) == "change_" => (before.slice(7, before.size), after)
    }
    val deletedAttrs = attrParams.toMap.filter {
      case (before, after) => after.isEmpty
    }
    val renamedAttrs = attrParams.toMap.filter {
      case (before, after) => after.nonEmpty && before != after
    }
    override def summary = {
      val renameStrings = renamedAttrs.map {
        case (before, after) =>
          s"${before} to ${after}"
      }
      val deleteStrings = deletedAttrs.keys
      val renameSummary = {
        if (renameStrings.nonEmpty) {
          s"Rename ${renameStrings.mkString(", ")}. "
        } else ""
      }
      val deleteSummary = {
        if (deleteStrings.nonEmpty) {
          s"Delete " + deleteStrings.mkString(", ")
        } else ""
      }
      renameSummary + deleteSummary
    }
    def apply() = {
      renamedAttrs.foreach {
        case (before, after) => {
          project.newVertexAttribute(
            after, project.vertexAttributes(before),
            project.viewer.getVertexAttributeNote(before)
          )
          project.vertexAttributes(before) = null
        }
      }
      deletedAttrs.foreach {
        case (before, after) => {
          project.deleteVertexAttribute(before)
        }
      }
    }
  })

  register("Set edge attribute icon")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Name", options = project.edgeAttrList),
      Param("icon", "Icon name"))
    def enabled = FEStatus.assert(project.edgeAttrList.nonEmpty, "No vertex attributes")
    override def summary = {
      val name = params("name")
      val icon = if (params("icon").nonEmpty) params("icon") else "nothing"
      s"Set icon for $name to $icon"
    }
    def apply() = {
      val name = params("name")
      val icon = params("icon")
      project.setElementMetadata(
        EdgeAttributeKind, name, MetadataNames.Icon,
        if (icon.nonEmpty) icon else null)
    }
  })

  register("Set scalar icon")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Name", options = project.scalarList),
      Param("icon", "Icon name"))
    def enabled = FEStatus.assert(project.scalarList.nonEmpty, "No scalars")
    override def summary = {
      val name = params("name")
      val icon = if (params("icon").nonEmpty) params("icon") else "nothing"
      s"Set icon for $name to $icon"
    }
    def apply() = {
      val name = params("name")
      val icon = params("icon")
      project.setElementMetadata(
        ScalarKind, name, MetadataNames.Icon,
        if (icon.nonEmpty) icon else null)
    }
  })

  register("Set segmentation icon")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Name", options = project.segmentationList),
      Param("icon", "Icon name"))
    def enabled = FEStatus.assert(project.segmentationList.nonEmpty, "No vertex attributes")
    override def summary = {
      val name = params("name")
      val icon = if (params("icon").nonEmpty) params("icon") else "nothing"
      s"Set icon for $name to $icon"
    }
    def apply() = {
      val name = params("name")
      val icon = params("icon")
      project.setElementMetadata(
        SegmentationKind, name, MetadataNames.Icon,
        if (icon.nonEmpty) icon else null)
    }
  })

  register("Set vertex attribute icon")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Name", options = project.vertexAttrList),
      Param("icon", "Icon name"))
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    override def summary = {
      val name = params("name")
      val icon = if (params("icon").nonEmpty) params("icon") else "nothing"
      s"Set icon for $name to $icon"
    }
    def apply() = {
      val name = params("name")
      val icon = params("icon")
      project.setElementMetadata(
        VertexAttributeKind, name, MetadataNames.Icon,
        if (icon.nonEmpty) icon else null)
    }
  })
}
