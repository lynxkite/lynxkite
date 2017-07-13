package com.lynxanalytics.biggraph.frontend_operations

import scala.collection.mutable
import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_operations.InducedEdgeBundle

class WorkflowOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.WorkflowOperations

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

  register("Project rejoin", List("target", "source"), List(projectOutput))(
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
      // .segmentation and .segmentation!edges
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
        p += attributeEditorParameter("apply_to_", "target", "Apply to (target)")
        p += attributeEditorParameter("apply_to_", "source", "Take from (source)")
        p
      }

      def getReachableAncestors(start: VertexSet): Map[VertexSet, Seq[EdgeBundle]] = {
        def canCarryAttributesFromAncestor(eb: EdgeBundle): Boolean = {
          // a -> a
          // b -> b
          //      c
          // d -> d
          //      e
          (eb.properties.isIdPreserving
            && eb.properties.isFunction
            && eb.properties.isReversedFunction
            && eb.properties.isEverywhereDefined)
        }

        val reachableAncestors = mutable.Map[VertexSet, Seq[EdgeBundle]]()
        val verticesToLookAt = mutable.Queue[VertexSet](start)
        reachableAncestors(start) = Seq[EdgeBundle]()
        while (verticesToLookAt.nonEmpty) {
          val src = verticesToLookAt.dequeue()
          val possibleOutgoingBundles =
            manager.outgoingBundles(src).filter(canCarryAttributesFromAncestor(_))
          for (eb <- possibleOutgoingBundles) {
            val dst = eb.dstVertexSet
            if (!reachableAncestors.contains(dst)) {
              val pathToSrc = reachableAncestors(src)
              val pathToDst = pathToSrc :+ eb
              reachableAncestors(dst) = pathToDst
              verticesToLookAt.enqueue(dst)
            }
          }
        }
        reachableAncestors.toMap
      }

      // Wrapper class to represent paths that lead to a common ancestor
      case class PathsToCommonAncestor(chain1: Seq[EdgeBundle], chain2: Seq[EdgeBundle])

      def computeChains(a: VertexSet,
                        b: VertexSet): Option[PathsToCommonAncestor] = {
        val aPaths = getReachableAncestors(a)
        val bPaths = getReachableAncestors(b)
        val possibleCommonAncestors = aPaths.keys.toSet & bPaths.keys.toSet
        if (possibleCommonAncestors.isEmpty) {
          None
        } else {
          val bestAncestor = possibleCommonAncestors.map {
            ancestorCandidate =>
              (ancestorCandidate, (aPaths(ancestorCandidate).length + bPaths(ancestorCandidate).length))
          }.toList.sortBy(_._2).head._1
          Some(PathsToCommonAncestor(aPaths(bestAncestor), bPaths(bestAncestor)))
        }
      }

      private val target = attributeEditor("target")
      private val source = attributeEditor("source")

      lazy val chain = computeChains(target.idSet.get, source.idSet.get)
      val hasTargetIdSet = target.idSet.isDefined
      val hasSourceIdSet = source.idSet.isDefined
      private val compatible = hasTargetIdSet && hasSourceIdSet && chain.isDefined

      private def attributesAreAvailable = source.names.nonEmpty
      private def segmentationsAreAvailable = {
        (target.kind == VertexAttributeKind) &&
          (source.kind == VertexAttributeKind) && (source.projectEditor.segmentationNames.nonEmpty)
      }

      if (compatible && attributesAreAvailable) {
        params += TagList("attrs", "Attributes", FEOption.list(source.names.toList))
      }
      if (compatible && segmentationsAreAvailable) {
        params += TagList("segs", "Segmentations", FEOption.list(source.projectEditor.segmentationNames.toList))
      }

      def enabled = (FEStatus(hasTargetIdSet, "No target input")
        && FEStatus(hasSourceIdSet, "No source input")
        && FEStatus(compatible, "Inputs are not compatible"))

      def apply() {
        val fromSourceToAncestor = chain.get.chain2
        val fromAncestorToTarget = chain.get.chain1.reverse
        if (attributesAreAvailable) {
          for (attrName <- splitParam("attrs")) {
            val attr = source.attributes(attrName)
            val note = source.getElementNote(attrName)
            val attrCommonAncestor =
              fromSourceToAncestor.foldLeft(attr) {
                (a, b) =>
                  val eb = b.reverse
                  graph_operations.PulledOverVertexAttribute.pullAttributeVia(a, eb)
              }
            val newAttr =
              fromAncestorToTarget.foldLeft(attrCommonAncestor) {
                (a, b) =>
                  graph_operations.PulledOverVertexAttribute.pullAttributeVia(a, b)
              }

            target.newAttribute(attrName, newAttr, note)
          }
        }

        if (segmentationsAreAvailable) {
          for (segmName <- splitParam("segs")) {
            val targetEditor = target.projectEditor
            val sourceEditor = source.projectEditor
            if (targetEditor.segmentationNames.contains(segmName)) {
              targetEditor.deleteSegmentation(segmName)
            }
            val sourceSegmentation = sourceEditor.existingSegmentation(segmName)
            val targetSegmentation = targetEditor.segmentation(segmName)
            val originalBelongsTo = sourceSegmentation.belongsTo
            val commonAncestorBelongsTo = fromSourceToAncestor.foldLeft(originalBelongsTo) {
              (currentBelongsTo, nextBelongsTo) =>
                val op = InducedEdgeBundle(induceDst = false)
                op(op.srcMapping, nextBelongsTo)(op.edges, currentBelongsTo).result.induced
            }
            val newBelongsTo = fromAncestorToTarget.foldLeft(commonAncestorBelongsTo) {
              (currentBelongsTo, nextBelongsTo) =>
                val reversed = nextBelongsTo.reverse
                val op = InducedEdgeBundle(induceDst = false)
                op(op.srcMapping, reversed)(op.edges, currentBelongsTo).result.induced
            }
            targetSegmentation.segmentationState = sourceSegmentation.segmentationState
            targetSegmentation.belongsTo = newBelongsTo
          }
        }
        project.state = target.projectEditor.rootEditor.state
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

  register("Take segmentation as base project")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    def enabled = FEStatus.enabled
    def apply() = {
      project.rootEditor.state = project.state
    }
  })

  register("Take edges as vertices")(new ProjectTransformation(_) {
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

  register("Take segmentation links as base project")(new ProjectTransformation(_) with SegOp {
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

  // TODO: Use dynamic inputs. #5820
  def registerSQLOp(name: String, inputs: List[String]): Unit = {
    registerOp(name, defaultIcon, category, inputs, List("table"), new TableOutputOperation(_) {
      override val params = new ParameterHolder(context) // No "apply_to" parameters.
      params += Code("sql", "SQL", defaultValue = "select * from vertices", language = "sql",
        enableTableBrowser = true)
      def enabled = FEStatus.enabled
      override def getOutputs() = {
        params.validate()
        val sql = params("sql")
        val protoTables = this.getInputTables()
        val result = graph_operations.ExecuteSQL.run(sql, protoTables)
        makeOutput(result)
      }
    })
  }

  registerSQLOp("SQL1", List("input"))

  for (inputs <- 2 to 3) {
    registerSQLOp(s"SQL$inputs", List("one", "two", "three").take(inputs))
  }
}
