// Frontend operations for projects.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.model
import play.api.libs.json

class Operations(env: SparkFreeEnvironment) extends OperationRepository(env) {
  val registries = Seq(
    new ProjectOperations(env),
    new MetaOperations(env),
    new ImportOperations(env),
    new BuildGraphOperations(env),
    new SubgraphOperations(env),
    new BuildSegmentationOperations(env),
    new UseSegmentationOperations(env),
    new StructureOperations(env),
    new ScalarOperations(env),
    new VertexAttributeOperations(env),
    new EdgeAttributeOperations(env),
    new AttributePropagationOperations(env),
    new GraphComputationOperations(env),
    new MachineLearningOperations(env),
    new ExportOperations(env),
    new PlotOperations(env),
    new VisualizationOperations(env))

  override val atomicOperations = registries.flatMap(_.operations).toMap
  override val atomicCategories = registries.flatMap(_.categories).toMap
}

class ProjectOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  protected val projectInput = "project" // The default input name, just to avoid typos.
  protected val projectOutput = "project"
  private val defaultIcon = "black_question_mark_ornament"

  def register(
    id: String,
    category: Category,
    factory: Context => ProjectTransformation): Unit = {
    registerOp(id, defaultIcon, category, List(projectInput), List(projectOutput), factory)
  }

  def register(
    id: String,
    category: Category,
    inputProjects: String*)(factory: Context => Operation): Unit = {
    registerOp(id, defaultIcon, category, inputProjects.toList, List(projectOutput), factory)
  }

  trait SegOp extends ProjectTransformation {
    protected def seg = project.asSegmentation
    protected def parent = seg.parent
    protected def addSegmentationParameters(): Unit
    if (project.isSegmentation) addSegmentationParameters()
  }

  // Categories
  val SpecialtyOperations = Category("Specialty operations", "green", icon = "glyphicon-book")
  val EdgeAttributesOperations =
    Category("Edge attribute operations", "blue", sortKey = "Attribute, edge")
  val VertexAttributesOperations =
    Category("Vertex attribute operations", "blue", sortKey = "Attribute, vertex")
  val GlobalOperations = Category("Global operations", "magenta", icon = "glyphicon-globe")
  val ImportOperations = Category("Import operations", "yellow", icon = "glyphicon-import")
  val MetricsOperations = Category("Graph metrics", "green", icon = "glyphicon-stats")
  val PropagationOperations =
    Category("Propagation operations", "green", icon = "glyphicon-fullscreen")
  val HiddenOperations = Category("Hidden operations", "black", visible = false)
  val DeprecatedOperations =
    Category("Deprecated operations", "red", visible = false, icon = "glyphicon-remove-sign")
  val CreateSegmentationOperations =
    Category("Create segmentation", "green", icon = "glyphicon-th-large")
  val StructureOperations = Category("Structure operations", "pink", icon = "glyphicon-asterisk")
  val MachineLearningOperations =
    Category("Machine learning operations", "pink ", icon = "glyphicon-knight")
  val UtilityOperations =
    Category("Utility operations", "green", icon = "glyphicon-wrench", sortKey = "zz")

  import OperationParams._

  register("Discard vertices", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasVertexSet && project.assertNotSegmentation
    def apply() = {
      project.vertexSet = null
    }
  })

  register("Take segmentation as base project", StructureOperations,
    new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters = {}
      def enabled = FEStatus.enabled
      def apply() = {
        project.rootEditor.state = project.state
      }
    })

  register("Take edges as vertices", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      val edgeBundle = project.edgeBundle
      val vertexAttrs = project.vertexAttributes.toMap
      val edgeAttrs = project.edgeAttributes.toMap
      project.scalars = Map()
      project.vertexSet = edgeBundle.idSet
      for ((name, attr) <- vertexAttrs) {
        project.newVertexAttribute(
          "src_" + name, graph_operations.VertexToEdgeAttribute.srcAttribute(attr, edgeBundle))
        project.newVertexAttribute(
          "dst_" + name, graph_operations.VertexToEdgeAttribute.dstAttribute(attr, edgeBundle))
      }
      for ((name, attr) <- edgeAttrs) {
        project.newVertexAttribute("edge_" + name, attr)
      }
    }
  })

  register("Take segmentation links as base project", StructureOperations,
    new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters = {}
      def enabled = FEStatus.enabled
      def apply() = {
        val root = project.rootEditor
        val baseAttrs = parent.vertexAttributes.toMap
        val segAttrs = project.vertexAttributes.toMap
        val belongsTo = seg.belongsTo
        root.scalars = Map()
        root.vertexSet = belongsTo.idSet
        for ((name, attr) <- baseAttrs) {
          root.newVertexAttribute(
            "base_" + name, graph_operations.VertexToEdgeAttribute.srcAttribute(attr, belongsTo))
        }
        for ((name, attr) <- segAttrs) {
          root.newVertexAttribute(
            "segment_" + name, graph_operations.VertexToEdgeAttribute.dstAttribute(attr, belongsTo))
        }
      }
    })

  register("Check cliques", UtilityOperations, new ProjectTransformation(_) with SegOp {
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

  register("Add gaussian vertex attribute", DeprecatedOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "random"),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.AddRandomAttribute(params("seed").toInt, "Standard Normal")
      project.newVertexAttribute(
        params("name"), op(op.vs, project.vertexSet).result.attr, help)
    }
  })

  register(
    "Create enhanced example graph", HiddenOperations)(new ProjectOutputOperation(_) {
      def enabled = FEStatus.enabled
      def apply() = {
        val g = graph_operations.EnhancedExampleGraph()().result
        project.vertexSet = g.vertices
        project.edgeBundle = g.edges
        for ((name, attr) <- g.vertexAttributes) {
          project.newVertexAttribute(name, attr)
        }
        project.newVertexAttribute("id", project.vertexSet.idAttribute)
        project.edgeAttributes = g.edgeAttributes.mapValues(_.entity)
      }
    })

  register("Discard edge attributes", EdgeAttributesOperations, new ProjectTransformation(_) {
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

  register("Discard vertex attributes", VertexAttributesOperations, new ProjectTransformation(_) {
    params += Choice("name", "Name", options = project.vertexAttrList, multipleChoice = true)
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
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

  register("Discard segmentation", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Discard scalars", GlobalOperations, new ProjectTransformation(_) {
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

  register("Rename edge attribute", UtilityOperations, new ProjectTransformation(_) {
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

  register("Rename vertex attribute", UtilityOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("before", "Old name", options = project.vertexAttrList),
      Param("after", "New name"))
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    override def summary = {
      val before = params("before")
      val after = params("after")
      s"Rename vertex attribute $before to $after"
    }
    def apply() = {
      assert(params("after").nonEmpty, "Please set the new attribute name.")
      project.newVertexAttribute(
        params("after"), project.vertexAttributes(params("before")),
        project.viewer.getVertexAttributeNote(params("before")))
      project.vertexAttributes(params("before")) = null
    }
  })

  register("Rename segmentation", UtilityOperations, new ProjectTransformation(_) {
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

  register("Rename scalar", UtilityOperations, new ProjectTransformation(_) {
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

  register("Set scalar icon", UtilityOperations, new ProjectTransformation(_) {
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

  register("Set vertex attribute icon", UtilityOperations, new ProjectTransformation(_) {
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

  register("Set edge attribute icon", UtilityOperations, new ProjectTransformation(_) {
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

  register("Set segmentation icon", UtilityOperations, new ProjectTransformation(_) {
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

  register("Copy edge attribute", UtilityOperations, new ProjectTransformation(_) {
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

  register("Copy vertex attribute", UtilityOperations, new ProjectTransformation(_) {
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

  register("Copy segmentation", UtilityOperations, new ProjectTransformation(_) {
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

  register("Copy scalar", UtilityOperations, new ProjectTransformation(_) {
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

  register("Join projects", StructureOperations, "a", "b")(
    new ProjectOutputOperation(_) {

      trait AttributeEditor {
        def projectEditor: ProjectEditor
        def kind: ElementKind
        def newAttribute(name: String, attr: Attribute[_], note: String = null): Unit
        def attributes: StateMapHolder[Attribute[_]]
        def idSet: Option[VertexSet]
        def names: Seq[String]

        def setElementNote(name: String, note: String) = {
          projectEditor.setElementNote(kind, name, note)
        }
        def getElementNote(name: String) = {
          projectEditor.viewer.getElementNote(kind, name)
        }
      }

      class VertexAttributeEditor(editor: ProjectEditor) extends AttributeEditor {
        override def projectEditor = editor
        override def kind = VertexAttributeKind
        override def attributes = editor.vertexAttributes
        override def newAttribute(name: String, attr: Attribute[_], note: String = null) = {
          editor.newVertexAttribute(name, attr, note)
        }
        override def idSet = Option(editor.vertexSet)
        override def names: Seq[String] = {
          editor.vertexAttributeNames
        }
      }

      class EdgeAttributeEditor(editor: ProjectEditor) extends AttributeEditor {
        override def projectEditor = editor
        override def kind = EdgeAttributeKind
        override def attributes = editor.edgeAttributes
        override def newAttribute(name: String, attr: Attribute[_], note: String = null) = {
          editor.newEdgeAttribute(name, attr, note)
        }
        override def idSet = Option(editor.edgeBundle).map(_.idSet)

        override def names: Seq[String] = {
          editor.edgeAttributeNames
        }
      }

      private val edgeMarker = "!edges"
      private def withEdgeMarker(s: String) = s + edgeMarker
      private def withoutEdgeMarker(s: String) = s.stripSuffix(edgeMarker)

      // We're using the same project editor for both
      // |segmentation and |segmentation!edges
      protected def attributeEditor(input: String): AttributeEditor = {
        val fullInputDesc = params("apply_to_" + input)
        val edgeEditor = fullInputDesc.endsWith(edgeMarker)
        val editorPath = SubProject.splitPipedPath(withoutEdgeMarker(fullInputDesc))

        val editor = context.inputs(input).project.offspringEditor(editorPath.tail)
        if (edgeEditor) new EdgeAttributeEditor(editor)
        else new VertexAttributeEditor(editor)
      }

      private def attributeEditorParameter(titlePrefix: String,
                                           input: String,
                                           title: String): OperationParams.SegmentationParam = {
        val param = titlePrefix + input
        val vertexAttributeEditors =
          context.inputs(input).project.segmentationsRecursively
        val edgeAttributeEditors =
          vertexAttributeEditors.map(x => FEOption(id = withEdgeMarker(x.id), title = withEdgeMarker(x.title)))

        val attributeEditors = (vertexAttributeEditors ++ edgeAttributeEditors).sortBy(_.title)
        // TODO: This should be something like an OperationParams.AttributeEditorParam
        OperationParams.SegmentationParam(param, title, attributeEditors)
      }

      override protected val params = {
        val p = new ParameterHolder(context)
        p += attributeEditorParameter("apply_to_", "a", "Apply to (a)")
        p += attributeEditorParameter("apply_to_", "b", "Take from (b)")
        p
      }

      // TODO: Extend this to allow filtered vertex sets to be compatible
      private def compatibleIdSets(a: Option[VertexSet], b: Option[VertexSet]): Boolean = {
        a.isDefined && b.isDefined && a.get == b.get
      }
      private def compatible = compatibleIdSets(left.idSet, right.idSet)

      private val left = attributeEditor("a")
      private val right = attributeEditor("b")

      private def attributesAreAvailable = right.names.nonEmpty
      private def segmentationsAreAvailable = {
        (left.kind == VertexAttributeKind) &&
          (right.kind == VertexAttributeKind) && (right.projectEditor.segmentationNames.nonEmpty)
      }

      if (compatible && attributesAreAvailable) {
        params += TagList("attrs", "Attributes", FEOption.list(right.names.toList))
      }
      if (compatible && segmentationsAreAvailable) {
        params += TagList("segs", "Segmentations", FEOption.list(right.projectEditor.segmentationNames.toList))
      }

      def enabled = FEStatus(compatible, "Left and right are not compatible")

      def apply() {
        if (attributesAreAvailable) {
          for (attrName <- splitParam("attrs")) {
            val attr = right.attributes(attrName)
            val note = right.getElementNote(attrName)
            left.newAttribute(attrName, attr, note)
          }
        }
        if (segmentationsAreAvailable) {
          for (segmName <- splitParam("segs")) {
            val leftEditor = left.projectEditor
            val rightEditor = right.projectEditor
            if (leftEditor.segmentationNames.contains(segmName)) {
              leftEditor.deleteSegmentation(segmName)
            }
            val rightSegm = rightEditor.existingSegmentation(segmName)
            val leftSegm = leftEditor.segmentation(segmName)
            leftSegm.segmentationState = rightSegm.segmentationState
          }
        }
        project.state = left.projectEditor.rootEditor.state
      }
    }
  )

  register("Union of projects", StructureOperations, "a", "b")(new ProjectOutputOperation(_) {
    override lazy val project = projectInput("a")
    lazy val other = projectInput("b")
    params += Param("id_attr", "ID attribute name", defaultValue = "new_id")
    def enabled = project.hasVertexSet && other.hasVertexSet

    def checkTypeCollision(other: ProjectViewer) = {
      val commonAttributeNames =
        project.vertexAttributes.keySet & other.vertexAttributes.keySet

      for (name <- commonAttributeNames) {
        val a1 = project.vertexAttributes(name)
        val a2 = other.vertexAttributes(name)
        assert(a1.typeTag.tpe =:= a2.typeTag.tpe,
          s"Attribute '$name' has conflicting types in the two projects: " +
            s"(${a1.typeTag.tpe} and ${a2.typeTag.tpe})")
      }

    }
    def apply(): Unit = {
      checkTypeCollision(other.viewer)
      val vsUnion = {
        val op = graph_operations.VertexSetUnion(2)
        op(op.vss, Seq(project.vertexSet, other.vertexSet)).result
      }

      val newVertexAttributes = unifyAttributes(
        project.vertexAttributes
          .map {
            case (name, attr) =>
              name -> attr.pullVia(vsUnion.injections(0).reverse)
          },
        other.vertexAttributes
          .map {
            case (name, attr) =>
              name -> attr.pullVia(vsUnion.injections(1).reverse)
          })
      val ebInduced = Option(project.edgeBundle).map { eb =>
        val op = graph_operations.InducedEdgeBundle()
        val mapping = vsUnion.injections(0)
        op(op.srcMapping, mapping)(op.dstMapping, mapping)(op.edges, project.edgeBundle).result
      }
      val otherEbInduced = Option(other.edgeBundle).map { eb =>
        val op = graph_operations.InducedEdgeBundle()
        val mapping = vsUnion.injections(1)
        op(op.srcMapping, mapping)(op.dstMapping, mapping)(op.edges, other.edgeBundle).result
      }

      val (newEdgeBundle, myEbInjection, otherEbInjection): (EdgeBundle, EdgeBundle, EdgeBundle) =
        if (ebInduced.isDefined && !otherEbInduced.isDefined) {
          (ebInduced.get.induced, ebInduced.get.embedding, null)
        } else if (!ebInduced.isDefined && otherEbInduced.isDefined) {
          (otherEbInduced.get.induced, null, otherEbInduced.get.embedding)
        } else if (ebInduced.isDefined && otherEbInduced.isDefined) {
          val idUnion = {
            val op = graph_operations.VertexSetUnion(2)
            op(
              op.vss,
              Seq(ebInduced.get.induced.idSet, otherEbInduced.get.induced.idSet))
              .result
          }
          val ebUnion = {
            val op = graph_operations.EdgeBundleUnion(2)
            op(
              op.ebs, Seq(ebInduced.get.induced.entity, otherEbInduced.get.induced.entity))(
                op.injections, idUnion.injections.map(_.entity)).result.union
          }
          (ebUnion,
            idUnion.injections(0).reverse.concat(ebInduced.get.embedding),
            idUnion.injections(1).reverse.concat(otherEbInduced.get.embedding))
        } else {
          (null, null, null)
        }
      val newEdgeAttributes = unifyAttributes(
        project.edgeAttributes
          .map {
            case (name, attr) => name -> attr.pullVia(myEbInjection)
          },
        other.edgeAttributes
          .map {
            case (name, attr) => name -> attr.pullVia(otherEbInjection)
          })

      project.vertexSet = vsUnion.union
      for ((name, attr) <- newVertexAttributes) {
        project.newVertexAttribute(name, attr) // Clear notes.
      }
      val idAttr = params("id_attr")
      project.newVertexAttribute(idAttr, project.vertexSet.idAttribute)
      project.edgeBundle = newEdgeBundle
      project.edgeAttributes = newEdgeAttributes
    }
  })

  register("Change project notes", UtilityOperations, new ProjectTransformation(_) {
    params += Param("notes", "New contents")
    def enabled = FEStatus.enabled
    def apply() = {
      project.notes = params("notes")
    }
  })

  register("Save UI status as graph attribute", UtilityOperations, new ProjectTransformation(_) {
    params ++= List(
      // In the future we may want a special kind for this so that users don't see JSON.
      Param("scalarName", "Name of new graph attribute"),
      Param("uiStatusJson", "UI status as JSON"))
    def enabled = FEStatus.enabled
    override def summary = {
      val scalarName = params("scalarName")
      s"Save visualization as $scalarName"
    }

    def apply() = {
      import UIStatusSerialization._
      val j = json.Json.parse(params("uiStatusJson"))
      val uiStatus = j.as[UIStatus]
      project.scalars(params("scalarName")) =
        graph_operations.CreateUIStatusScalar(uiStatus).result.created
    }
  })

  register("Import metagraph", StructureOperations, new ProjectTransformation(_) {
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
      project.newVertexAttribute("id", project.vertexSet.idAttribute)
      project.edgeBundle = mg.es
      project.newEdgeAttribute("kind", mg.eKind)
      project.newEdgeAttribute("name", mg.eName)
    }
  })

  protected def segmentationSizesSquareSum(seg: SegmentationEditor, parent: ProjectEditor)(
    implicit manager: MetaGraphManager): Scalar[_] = {
    val size = aggregateViaConnection(
      seg.belongsTo,
      AttributeWithLocalAggregator(parent.vertexSet.idAttribute, "count")
    )
    val sizeSquare: Attribute[Double] = {
      val op = graph_operations.DeriveJSDouble(
        JavaScript("size * size"),
        Seq("size"))
      op(
        op.attrs,
        graph_operations.VertexAttributeToJSValue.seq(size)).result.attr
    }
    aggregate(AttributeWithAggregator(sizeSquare, "sum"))
  }

  protected def segmentationSizesProductSum(seg: SegmentationEditor, parent: ProjectEditor)(
    implicit manager: MetaGraphManager): Scalar[_] = {
    val size = aggregateViaConnection(
      seg.belongsTo,
      AttributeWithLocalAggregator(parent.vertexSet.idAttribute, "count")
    )
    val srcSize = graph_operations.VertexToEdgeAttribute.srcAttribute(size, seg.edgeBundle)
    val dstSize = graph_operations.VertexToEdgeAttribute.dstAttribute(size, seg.edgeBundle)
    val sizeProduct: Attribute[Double] = {
      val op = graph_operations.DeriveJSDouble(
        JavaScript("src_size * dst_size"),
        Seq("src_size", "dst_size"))
      op(
        op.attrs,
        graph_operations.VertexAttributeToJSValue.seq(srcSize, dstSize)).result.attr
    }
    aggregate(AttributeWithAggregator(sizeProduct, "sum"))
  }

  // TODO: Use dynamic inputs. #5820
  def registerSQLOp(name: String, inputs: List[String]): Unit = {
    registerOp(name, defaultIcon, UtilityOperations, inputs, List("table"), new TableOutputOperation(_) {
      override val params = new ParameterHolder(context) // No "apply_to" parameters.
      params += Code("sql", "SQL", defaultValue = "select * from vertices", language = "sql")
      def enabled = FEStatus.enabled
      override def getOutputs() = {
        params.validate()
        val sql = params("sql")
        val protoTables = this.getInputTables()
        val tables = ProtoTable.minimize(sql, protoTables).mapValues(_.toTable)
        val result = graph_operations.ExecuteSQL.run(sql, tables)
        makeOutput(result)
      }
    })
  }

  registerSQLOp("SQL1", List("input"))

  for (inputs <- 2 to 3) {
    registerSQLOp(s"SQL$inputs", List("one", "two", "three").take(inputs))
  }

  protected def getShapeFilePath(params: ParameterHolder): String = {
    val shapeFilePath = params("shapefile")
    assert(listShapefiles().exists(f => f.id == shapeFilePath),
      "Shapefile deleted, please choose another.")
    shapeFilePath
  }

  protected def listShapefiles(): List[FEOption] = {
    import java.io.File
    def metaDir = new File(env.metaGraphManager.repositoryPath).getParent
    val shapeDir = s"$metaDir/resources/shapefiles/"
    def lsR(f: File): Array[File] = {
      val files = f.listFiles()
      if (files == null)
        return Array.empty
      files.filter(_.getName.endsWith(".shp")) ++ files.filter(_.isDirectory).flatMap(lsR)
    }
    lsR(new File(shapeDir)).toList.map(f =>
      FEOption(f.getPath, f.getPath.substring(shapeDir.length)))
  }

  def computeSegmentSizes(segmentation: SegmentationEditor): Attribute[Double] = {
    val op = graph_operations.OutDegree()
    op(op.es, segmentation.belongsTo.reverse).result.outDegree
  }

  def toDouble(attr: Attribute[_]): Attribute[Double] = {
    if (attr.is[String])
      attr.runtimeSafeCast[String].asDouble
    else if (attr.is[Long])
      attr.runtimeSafeCast[Long].asDouble
    else if (attr.is[Int])
      attr.runtimeSafeCast[Int].asDouble
    else
      throw new AssertionError(s"Unexpected type (${attr.typeTag}) on $attr")
  }

  def parseAggregateParams(params: ParameterHolder) = {
    val aggregate = "aggregate_(.*)".r
    params.toMap.toSeq.collect {
      case (aggregate(attr), choices) if choices.nonEmpty => attr -> choices
    }.flatMap {
      case (attr, choices) => choices.split(",", -1).map(attr -> _)
    }
  }
  def aggregateParams(
    attrs: Iterable[(String, Attribute[_])],
    needsGlobal: Boolean = false,
    weighted: Boolean = false): List[OperationParameterMeta] = {
    val sortedAttrs = attrs.toList.sortBy(_._1)
    sortedAttrs.toList.map {
      case (name, attr) =>
        val options = if (attr.is[Double]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            FEOption.list("weighted_average", "by_max_weight", "by_min_weight", "weighted_sum")
          } else if (needsGlobal) {
            FEOption.list(
              "average", "count", "count_distinct", "count_most_common", "first", "max", "min", "most_common",
              "std_deviation", "sum")

          } else {
            FEOption.list(
              "average", "count", "count_distinct", "count_most_common", "first", "max", "median", "min", "most_common",
              "set", "std_deviation", "sum", "vector")
          }
        } else if (attr.is[String]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            FEOption.list("by_max_weight", "by_min_weight")
          } else if (needsGlobal) {
            FEOption.list("count", "count_distinct", "first", "most_common", "count_most_common")
          } else {
            FEOption.list(
              "count", "count_distinct", "first", "most_common", "count_most_common", "majority_50", "majority_100",
              "vector", "set")
          }
        } else {
          if (weighted) { // At the moment all weighted aggregators are global.
            FEOption.list("by_max_weight", "by_min_weight")
          } else if (needsGlobal) {
            FEOption.list("count", "count_distinct", "first", "most_common", "count_most_common")
          } else {
            FEOption.list("count", "count_distinct", "first", "median", "most_common", "count_most_common", "set", "vector")
          }
        }
        TagList(s"aggregate_$name", name, options = options)
    }
  }

  // Performs AggregateAttributeToScalar.
  protected def aggregate[From, Intermediate, To](
    attributeWithAggregator: AttributeWithAggregator[From, Intermediate, To]): Scalar[To] = {
    val op = graph_operations.AggregateAttributeToScalar(attributeWithAggregator.aggregator)
    op(op.attr, attributeWithAggregator.attr).result.aggregated
  }

  // Performs AggregateByEdgeBundle.
  protected def aggregateViaConnection[From, To](
    connection: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateByEdgeBundle(attributeWithAggregator.aggregator)
    op(op.connection, connection)(op.attr, attributeWithAggregator.attr).result.attr
  }
  private def mergeEdgesWithKey[T](edgesAsAttr: Attribute[(ID, ID)], keyAttr: Attribute[T]) = {
    val edgesAndKey: Attribute[((ID, ID), T)] = edgesAsAttr.join(keyAttr)
    val op = graph_operations.MergeVertices[((ID, ID), T)]()
    op(op.attr, edgesAndKey).result
  }

  protected def mergeEdges(edgesAsAttr: Attribute[(ID, ID)]) = {
    val op = graph_operations.MergeVertices[(ID, ID)]()
    op(op.attr, edgesAsAttr).result
  }

  // Common code for operations "merge parallel edges" and "merge parallel edges by key"
  protected def applyMergeParallelEdges(
    project: ProjectEditor, params: ParameterHolder, byKey: Boolean) = {

    val edgesAsAttr = {
      val op = graph_operations.EdgeBundleAsAttribute()
      op(op.edges, project.edgeBundle).result.attr
    }

    val mergedResult =
      if (byKey) {
        val keyAttr = project.edgeAttributes(params("key"))
        mergeEdgesWithKey(edgesAsAttr, keyAttr)
      } else {
        mergeEdges(edgesAsAttr)
      }

    val newEdges = {
      val op = graph_operations.PulledOverEdges()
      op(op.originalEB, project.edgeBundle)(op.injection, mergedResult.representative)
        .result.pulledEB
    }
    val oldAttrs = project.edgeAttributes.toMap
    project.edgeBundle = newEdges

    for ((attr, choice) <- parseAggregateParams(params)) {
      project.edgeAttributes(s"${attr}_${choice}") =
        aggregateViaConnection(
          mergedResult.belongsTo,
          AttributeWithLocalAggregator(oldAttrs(attr), choice))
    }
    if (byKey) {
      val key = params("key")
      project.edgeAttributes(key) =
        aggregateViaConnection(mergedResult.belongsTo,
          AttributeWithLocalAggregator(oldAttrs(key), "most_common"))
    }
  }

  // Performs AggregateFromEdges.
  protected def aggregateFromEdges[From, To](
    edges: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateFromEdges(attributeWithAggregator.aggregator)
    val res = op(op.edges, edges)(op.eattr, attributeWithAggregator.attr).result
    res.dstAttr
  }

  def stripDuplicateEdges(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.StripDuplicateEdgesFromBundle()
    op(op.es, eb).result.unique
  }

  object Direction {
    // Options suitable when edge attributes are involved.
    val attrOptions = FEOption.list("incoming edges", "outgoing edges", "all edges")
    def attrOptionsWithDefault(default: String): List[FEOption] = {
      assert(attrOptions.map(_.id).contains(default), s"$default not in $attrOptions")
      FEOption.list(default) ++ attrOptions.filter(_.id != default)
    }
    // Options suitable when only neighbors are involved.
    val neighborOptions = FEOption.list(
      "in-neighbors", "out-neighbors", "all neighbors", "symmetric neighbors")
    // Options suitable when edge attributes are not involved.
    val options = attrOptions ++ FEOption.list("symmetric edges") ++ neighborOptions
    // Neighborhood directions correspond to these
    // edge directions, but they also retain only one A->B edge in
    // the output edgeBundle
    private val neighborOptionMapping = Map(
      "in-neighbors" -> "incoming edges",
      "out-neighbors" -> "outgoing edges",
      "all neighbors" -> "all edges",
      "symmetric neighbors" -> "symmetric edges"
    )
  }
  case class Direction(direction: String, origEB: EdgeBundle, reversed: Boolean = false) {
    val unchangedOut: (EdgeBundle, Option[EdgeBundle]) = (origEB, None)
    val reversedOut: (EdgeBundle, Option[EdgeBundle]) = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, origEB).result
      (res.esBA, Some(res.injection))
    }
    private def computeEdgeBundleAndPullBundleOpt(dir: String): (EdgeBundle, Option[EdgeBundle]) = {
      dir match {
        case "incoming edges" => if (reversed) reversedOut else unchangedOut
        case "outgoing edges" => if (reversed) unchangedOut else reversedOut
        case "all edges" =>
          val op = graph_operations.AddReversedEdges()
          val res = op(op.es, origEB).result
          (res.esPlus, Some(res.newToOriginal))
        case "symmetric edges" =>
          // Use "null" as the injection because it is an error to use
          // "symmetric edges" with edge attributes.
          (origEB.makeSymmetric, Some(null))
      }
    }

    val (edgeBundle, pullBundleOpt): (EdgeBundle, Option[EdgeBundle]) = {
      if (Direction.neighborOptionMapping.contains(direction)) {
        val (eB, pBO) = computeEdgeBundleAndPullBundleOpt(Direction.neighborOptionMapping(direction))
        (stripDuplicateEdges(eB), pBO)
      } else {
        computeEdgeBundleAndPullBundleOpt(direction)
      }
    }

    def pull[T](attribute: Attribute[T]): Attribute[T] = {
      pullBundleOpt.map(attribute.pullVia(_)).getOrElse(attribute)
    }
  }

  protected def unifyAttributeT[T](a1: Attribute[T], a2: Attribute[_]): Attribute[T] = {
    a1.fallback(a2.runtimeSafeCast(a1.typeTag))
  }
  def unifyAttribute(a1: Attribute[_], a2: Attribute[_]): Attribute[_] = {
    unifyAttributeT(a1, a2)
  }

  def unifyAttributes(
    as1: Iterable[(String, Attribute[_])],
    as2: Iterable[(String, Attribute[_])]): Map[String, Attribute[_]] = {

    val m1 = as1.toMap
    val m2 = as2.toMap
    m1.keySet.union(m2.keySet)
      .map(k => k -> (m1.get(k) ++ m2.get(k)).reduce(unifyAttribute _))
      .toMap
  }

  def newScalar(data: String): Scalar[String] = {
    val op = graph_operations.CreateStringScalar(data)
    op.result.created
  }

}

object JSUtilities {
  // Listing the valid characters for JS variable names. The \\p{*} syntax is for specifying
  // Unicode categories for scala regex.
  // For more information about the valid variable names in JS please consult:
  // http://es5.github.io/x7.html#x7.6
  val validJSCharacters = "_$\\p{Lu}\\p{Ll}\\p{Lt}\\p{Lm}\\p{Lo}\\p{Nl}\\p{Mn}" +
    "\\p{Mc}\\p{Nd}\\p{Pc}\\u200C\\u200D\\\\"
  val validJSFirstCharacters = "_$\\p{Lu}\\p{Ll}\\p{Lt}\\p{Lm}\\p{Lo}\\p{Nl}\\\\"

  def collectIdentifiers[T <: MetaGraphEntity](
    holder: StateMapHolder[T],
    expr: String,
    prefix: String = ""): IndexedSeq[(String, T)] = {
    holder.filter {
      case (name, _) => containsIdentifierJS(expr, prefix + name)
    }.toIndexedSeq
  }

  // Whether a string can be a JavaScript identifier.
  def canBeValidJSIdentifier(identifier: String): Boolean = {
    val re = s"^[${validJSFirstCharacters}][${validJSCharacters}]*$$"
    identifier.matches(re)
  }

  // Whether a JavaScript expression contains a given identifier.
  // It's a best-effort implementation with no guarantees of correctness.
  def containsIdentifierJS(expr: String, identifier: String): Boolean = {
    if (!canBeValidJSIdentifier(identifier)) {
      false
    } else {
      val quotedIdentifer = java.util.regex.Pattern.quote(identifier)
      val re = s"(?s)(^|.*[^$validJSCharacters])${quotedIdentifer}($$|[^$validJSCharacters].*)"
      expr.matches(re)
    }
  }
}
