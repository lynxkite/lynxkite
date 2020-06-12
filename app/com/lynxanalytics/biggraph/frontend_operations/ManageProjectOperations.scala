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

  register("Copy graph attribute")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Old name", options = project.scalarList),
      Param("destination", "New name"))
    def enabled = FEStatus.assert(project.scalarList.nonEmpty, "No graph attributes")
    override def summary = {
      val from = params("name")
      val to = params("destination")
      s"Copy $from to $to"
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
    def enabled = FEStatus.enabled
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

  register("Discard graph attributes")(new ProjectTransformation(_) {
    params += Choice("name", "Name", options = project.scalarList, multipleChoice = true)
    def enabled = FEStatus.enabled
    override def summary = {
      val names = params("name").replace(",", ", ")
      s"Discard $names"
    }
    def apply() = {
      for (param <- splitParam("name")) {
        project.deleteScalar(param)
      }
    }
  })

  register("Discard segmentation")(new ProjectTransformation(_) {
    params += Choice("name", "Name", options = project.segmentationList)
    def enabled = FEStatus.enabled
    override def summary = {
      val name = params("name")
      s"Discard segmentation: $name"
    }
    def apply() = {
      project.deleteSegmentation(params("name"))
    }
  })

  register("Discard vertex attributes")(new ProjectTransformation(_) {
    params += Choice("name", "Name", options = project.vertexAttrList, multipleChoice = true)
    def enabled = FEStatus.enabled
    override def summary = {
      val names = params("name").replace(",", ", ")
      s"Discard vertex attributes: $names"
    }
    def apply() = {
      for (param <- splitParam("name")) {
        project.deleteVertexAttribute(param)
      }
    }
  })

  register("Rename edge attributes")(new ProjectTransformation(_) {
    params += new DummyParam("text", "The new names for each attribute:")
    params ++= project.edgeAttrList.map {
      attr => Param(s"change_${attr.id}", attr.id, defaultValue = attr.id)
    }
    def enabled = project.hasEdgeBundle
    val attrParams = params.toMap.collect {
      case (before, after) if before.startsWith("change_") => (before.stripPrefix("change_"), after)
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
      val deleteSummary =
        if (deleteStrings.isEmpty) "" else s"Delete ${deleteStrings.mkString(", ")}"
      val renameSummary =
        if (renameStrings.isEmpty) ""
        else if (deleteStrings.isEmpty) s"Rename ${renameStrings.mkString(", ")}"
        else s" and rename ${renameStrings.mkString(", ")}"
      deleteSummary + renameSummary
    }
    def apply() = {
      deletedAttrs.foreach {
        case (before, after) => {
          project.deleteEdgeAttribute(before)
        }
      }
      renamedAttrs.foreach {
        case (before, after) => {
          project.newEdgeAttribute(
            after,
            project.edgeAttributes(before),
            project.viewer.getEdgeAttributeNote(before))
          project.edgeAttributes(before) = null
        }
      }
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

  register("Rename vertex attributes")(new ProjectTransformation(_) {
    params += new DummyParam("text", "The new names for each attribute:")
    params ++= project.vertexAttrList.map {
      attr => Param(s"change_${attr.id}", attr.id, defaultValue = attr.id)
    }
    def enabled = project.hasVertexSet
    val attrParams = params.toMap.collect {
      case (before, after) if before.startsWith("change_") => (before.stripPrefix("change_"), after)
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
      val deleteSummary =
        if (deleteStrings.isEmpty) "" else s"Delete ${deleteStrings.mkString(", ")}"
      val renameSummary =
        if (renameStrings.isEmpty) ""
        else if (deleteStrings.isEmpty) s"Rename ${renameStrings.mkString(", ")}"
        else s" and rename ${renameStrings.mkString(", ")}"
      deleteSummary + renameSummary
    }
    def apply() = {
      deletedAttrs.foreach {
        case (before, after) => {
          project.deleteVertexAttribute(before)
        }
      }
      renamedAttrs.foreach {
        case (before, after) => {
          project.newVertexAttribute(
            after,
            project.vertexAttributes(before),
            project.viewer.getVertexAttributeNote(before))
          project.vertexAttributes(before) = null
        }
      }
    }
  })

  register("Rename graph attributes")(new ProjectTransformation(_) {
    params += new DummyParam("text", "The new names for each attribute:")
    params ++= project.scalarList.map {
      s => Param(s"change_${s.id}", s.id, defaultValue = s.id)
    }
    def enabled = FEStatus.enabled
    val scalarParams = params.toMap.collect {
      case (before, after) if before.startsWith("change_") => (before.stripPrefix("change_"), after)
    }
    val deletedScalars = scalarParams.toMap.filter {
      case (before, after) => after.isEmpty
    }
    val renamedScalars = scalarParams.toMap.filter {
      case (before, after) => after.nonEmpty && before != after
    }
    override def summary = {
      val renameStrings = renamedScalars.map {
        case (before, after) =>
          s"${before} to ${after}"
      }
      val deleteStrings = deletedScalars.keys
      val deleteSummary =
        if (deleteStrings.isEmpty) "" else s"Delete ${deleteStrings.mkString(", ")}"
      val renameSummary =
        if (renameStrings.isEmpty) ""
        else if (deleteStrings.isEmpty) s"Rename ${renameStrings.mkString(", ")}"
        else s" and rename ${renameStrings.mkString(", ")}"
      deleteSummary + renameSummary
    }
    def apply() = {
      deletedScalars.foreach {
        case (before, after) => {
          project.deleteScalar(before)
        }
      }
      renamedScalars.foreach {
        case (before, after) => {
          project.newScalar(
            after,
            project.scalars(before),
            project.viewer.getScalarNote(before))
          project.scalars(before) = null
        }
      }
    }
  })

  register("Set edge attribute icons")(new ProjectTransformation(_) {
    params += new DummyParam("text", "The icons for each attribute:")
    params ++= project.edgeAttrList.map {
      attr => Param(s"icon_for_${attr.id}", attr.id)
    }
    def enabled = project.hasEdgeBundle
    val attrParams: Map[String, String] = params.toMap.collect {
      case (name, value) if name.startsWith("icon_for_") => {
        (name.stripPrefix("icon_for_"), value)
      }
    }
    override def summary = {
      val parts = attrParams.collect {
        case (name, icon) if icon.nonEmpty => s"${name} to ${icon}"
      }
      s"Set icon for ${parts.mkString(", ")}"
    }
    def apply() = {
      for ((name, icon) <- attrParams.toMap) {
        val attr = project.edgeAttributes(name)
        project.setElementMetadata(
          EdgeAttributeKind, name, MetadataNames.Icon,
          if (icon.nonEmpty) icon else null)
      }
    }
  })

  register("Set graph attribute icon")(new ProjectTransformation(_) {
    params ++= List(
      Choice("name", "Name", options = project.scalarList),
      Param("icon", "Icon name"))
    def enabled = FEStatus.assert(project.scalarList.nonEmpty, "No graph attributes")
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

  register("Set vertex attribute icons")(new ProjectTransformation(_) {
    params += new DummyParam("text", "The icons for each attribute:")
    params ++= project.vertexAttrList.map {
      attr => Param(s"icon_for_${attr.id}", attr.id)
    }
    def enabled = project.hasVertexSet
    val attrParams: Map[String, String] = params.toMap.collect {
      case (name, value) if name.startsWith("icon_for_") => {
        (name.stripPrefix("icon_for_"), value)
      }
    }
    override def summary = {
      val parts = attrParams.collect {
        case (name, icon) if icon.nonEmpty => s"${name} to ${icon}"
      }
      s"Set icon for ${parts.mkString(", ")}"
    }
    def apply() = {
      for ((name, icon) <- attrParams.toMap) {
        val attr = project.vertexAttributes(name)
        project.setElementMetadata(
          VertexAttributeKind, name, MetadataNames.Icon,
          if (icon.nonEmpty) icon else null)
      }
    }
  })
}
