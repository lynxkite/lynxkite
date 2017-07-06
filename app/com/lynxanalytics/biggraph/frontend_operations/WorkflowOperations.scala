package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._

class WorkflowOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.WorkflowOperations

  def register(id: String, inputs: List[String], outputs: List[String])(factory: Context => Operation): Unit = {
    registerOp(id, defaultIcon, category, inputs, outputs, factory)
  }

  def register(id: String, icon: String, inputs: List[String], outputs: List[String])(factory: Context => Operation): Unit = {
    registerOp(id, icon, category, inputs, outputs, factory)
  }

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Comment", List(), List())(new DecoratorOperation(_) {
    params += Code("comment", "Comment", language = "plain_text")
  })

  register("Input", "black_down-pointing_triangle", List(), List("input"))(
    new SimpleOperation(_) {
      params += Param("name", "Name")
      override def summary = s"Input ${params("name")}"
    })

  register("Output", "black_up-pointing_triangle", List("output"), List())(
    new SimpleOperation(_) {
      params += Param("name", "Name")
      override def summary = s"Output ${params("name")}"
    })

  register("Project rejoin", List("a", "b"), List(projectOutput))(
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

  register("Project union", List("a", "b"), List(projectOutput))(new ProjectOutputOperation(_) {
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

  // TODO: Use dynamic inputs. #5820
  def registerSQLOp(name: String, inputs: List[String]): Unit = {
    registerOp(name, defaultIcon, category, inputs, List("table"), new TableOutputOperation(_) {
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
}
