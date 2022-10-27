package com.lynxanalytics.biggraph.frontend_operations

import scala.collection.mutable
import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.Environment
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_operations.InducedEdgeBundle
import com.lynxanalytics.biggraph.{logger => log}

class WorkflowOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.WorkflowOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Comment", List(), List(), icon = "comment-alt")(new DecoratorOperation(_) {
    params += Code("comment", "Comment", language = "plain_text")
  })

  register("Input", List(), List("input"), "black_down-pointing_triangle")(
    new SimpleOperation(_) {
      params += Param("name", "Name")
      override def summary = s"Input ${params("name")}"
      override def getOutputs() = {
        // This will be replaced by a different output when inside a parent workspace.
        throw new AssertionError("Unconnected")
      }
    })

  register("Output", List("output"), List(), "black_up-pointing_triangle")(
    new SimpleOperation(_) {
      params += Param("name", "Name")
      override def summary = s"Output ${params("name")}"
    })

  register("Graph rejoin", List("target", "source"), List(projectOutput))(
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

      private def attributeEditorParameter(
          titlePrefix: String,
          input: String,
          title: String): OperationParams.SegmentationParam = {
        val param = titlePrefix + input
        val vertexAttributeEditors =
          context.inputs(input).project.segmentationsRecursively
        val edgeAttributeEditors =
          vertexAttributeEditors.map(x => FEOption(id = withEdgeMarker(x.id), title = withEdgeMarker(x.title)))

        val attributeEditors = (vertexAttributeEditors ++ edgeAttributeEditors).sortBy(_.id)
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

      def computeChains(
          a: VertexSet,
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

      private def edgesCanBeCarriedOver = {
        (target.kind == VertexAttributeKind) &&
        (source.kind == VertexAttributeKind) &&
        (source.projectEditor.hasEdgeBundle.enabled)
      }

      if (compatible && attributesAreAvailable) {
        params += TagList("attrs", "Attributes", FEOption.list(source.names.toList))
      }
      if (compatible && segmentationsAreAvailable) {
        params += TagList("segs", "Segmentations", FEOption.list(source.projectEditor.segmentationNames.toList))
      }

      if (compatible && edgesCanBeCarriedOver) {
        params += Choice("edge", "Copy edges", FEOption.list("no", "yes"))
      }

      def enabled = (FEStatus(hasTargetIdSet, "No target input")
        && FEStatus(hasSourceIdSet, "No source input")
        && FEStatus(compatible, "Inputs are not compatible"))

      private def copyAttributesViaCommonAncestor(
          target: AttributeEditor,
          source: AttributeEditor,
          fromSourceToAncestor: Seq[EdgeBundle],
          fromAncestorToTarget: Seq[EdgeBundle],
          attributeNames: Seq[String]): Unit = {
        for (attrName <- attributeNames) {
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

      def apply() {
        val fromSourceToAncestor = chain.get.chain2
        val fromAncestorToTarget = chain.get.chain1.reverse
        if (attributesAreAvailable) {
          copyAttributesViaCommonAncestor(
            target,
            source,
            fromSourceToAncestor,
            fromAncestorToTarget,
            splitParam("attrs"))
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

        if (edgesCanBeCarriedOver && params("edge") == "yes") {
          val targetEditor = target.projectEditor
          val sourceEditor = source.projectEditor
          val sourceEdges = sourceEditor.edgeBundle

          val commonAncestorEdges = fromSourceToAncestor.foldLeft(sourceEdges) {
            (graphEdges, chainBundle) =>
              val op = InducedEdgeBundle(induceSrc = true, induceDst = true)
              op(op.srcMapping, chainBundle)(op.dstMapping, chainBundle)(op.edges, graphEdges).result.induced
          }
          val newEdges = fromAncestorToTarget.foldLeft(commonAncestorEdges) {
            (graphEdges, chainBundle) =>
              val op = InducedEdgeBundle(induceSrc = true, induceDst = true)
              val reversed = chainBundle.reverse
              op(op.srcMapping, reversed)(op.dstMapping, reversed)(op.edges, graphEdges).result.induced
          }
          val sourceEdgeEditor = new EdgeAttributeEditor(sourceEditor)
          val targetEdgeEditor = new EdgeAttributeEditor(targetEditor)
          targetEdgeEditor.projectEditor.edgeBundle = newEdges
          val edgeChain = computeChains(sourceEdgeEditor.idSet.get, targetEdgeEditor.idSet.get)
          assert(edgeChain.isDefined) // This should not hit us, right? We have just created a path.
          copyAttributesViaCommonAncestor(
            targetEdgeEditor,
            sourceEdgeEditor,
            edgeChain.get.chain1,
            edgeChain.get.chain2.reverse,
            sourceEdgeEditor.names)
        }

        project.state = target.projectEditor.rootEditor.state
      }
    })

  register("Graph union", List("a", "b"), List(projectOutput))(new ProjectOutputOperation(_) {
    override lazy val project = projectInput("a")
    lazy val other = projectInput("b")
    def enabled = project.hasVertexSet && other.hasVertexSet

    def apply(): Unit = {
      mergeInto(project, other.viewer)
    }
  })

  register("Compute inputs", List("input"), List())(new TriggerableOperation(_) {
    params += TriggerBoxParam("compute", "Compute input GUIDs", "Computation finished.")

    override def trigger(wc: WorkspaceController, gdc: GraphDrawingController) = {
      gdc.getComputeBoxResult(getGUIDs("input"))
    }
  })

  register("Take segmentation as base graph")(new ProjectTransformation(_) with SegOp {
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
      project.vertexSet = edgeBundle.idSet
      for ((name, attr) <- vertexAttrs) {
        project.newVertexAttribute(
          "src_" + name,
          graph_operations.VertexToEdgeAttribute.srcAttribute(attr, edgeBundle))
        project.newVertexAttribute(
          "dst_" + name,
          graph_operations.VertexToEdgeAttribute.dstAttribute(attr, edgeBundle))
      }
      for ((name, attr) <- edgeAttrs) {
        project.newVertexAttribute("edge_" + name, attr)
      }
    }
  })

  register("Take segmentation links as base graph")(new ProjectTransformation(_) with SegOp {
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
          "base_" + name,
          graph_operations.VertexToEdgeAttribute.srcAttribute(attr, belongsTo))
      }
      for ((name, attr) <- segAttrs) {
        root.newVertexAttribute(
          "segment_" + name,
          graph_operations.VertexToEdgeAttribute.dstAttribute(attr, belongsTo))
      }
    }
  })

  // TODO: Use dynamic inputs. #5820
  def registerSQLOp(name: String, inputs: List[String]): Unit = {
    registerOp(
      name,
      defaultIcon,
      category,
      inputs,
      List("table"),
      new TableOutputOperation(_) {
        override val params = new ParameterHolder(context) // No "apply_to" parameters.
        params += Param("summary", "Summary", defaultValue = "SQL")
        params += Param("input_names", "Input names", defaultValue = inputs.mkString(", "))
        params += Code(
          "sql",
          "SQL",
          defaultValue = s"select * from $defaultTableName\n",
          language = "sql",
          enableTableBrowser = true)
        params += Choice("persist", "Persist result", options = FEOption.noyes)
        override def summary = params("summary")
        def enabled = FEStatus.enabled
        def defaultTableName = {
          val first = inputNames.head
          val state = context.inputs(inputs.head)
          val name =
            if (state.isProject) {
              if (inputNames.length == 1) "vertices"
              else first + ".vertices"
            } else first
          val simple = "[a-zA-Z0-9]*".r
          name match {
            case simple() => name
            case _ => s"`$name`"
          }
        }
        lazy val renaming: Map[String, String] = {
          inputs.zip(inputNames).toMap
        }
        lazy val inputNames = {
          val names = params("input_names").split(",", -1).map(_.trim)
          assert(
            names.length == inputs.length,
            s"Mismatched input name list: ${params("input_names")}")
          names
        }
        override def getOutputs() = {
          params.validate()
          val sql = params("sql")
          val protoTables = this.getInputTables(renaming)
          val result = graph_operations.ExecuteSQL.run(sql, protoTables)
          if (params("persist") == "yes") makeOutput(result.saved)
          else makeOutput(result)
        }
      },
    )
  }

  registerSQLOp("SQL1", List("input"))

  for (inputs <- 2 to 10) {
    val numbers =
      List("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")
    registerSQLOp(s"SQL$inputs", numbers.take(inputs))
  }

  registerOp(
    "Filter with SQL",
    "filter",
    category,
    List("input"),
    List("output"),
    new SmartOperation(_) {
      private val input = context.inputs("input")
      if (input.isProject) {
        params ++= List(
          Code("vertex_filter", "Vertex filter", language = "sql"),
          Code("edge_filter", "Edge filter", language = "sql"))
      } else {
        assert(input.isTable, "Input must be a graph or a table.")
        params ++= List(
          Code("filter", "Filter", language = "sql"))
      }
      override def summary = {
        if (input.isProject) {
          val vf = params("vertex_filter").trim
          val ef = params("edge_filter").trim
          if (vf.isEmpty && ef.isEmpty) "Filter with SQL"
          else if (vf.nonEmpty && ef.nonEmpty) s"Filter to $vf; $ef"
          else s"Filter to $vf$ef"
        } else {
          val f = params("filter").trim
          if (f.isEmpty) "Filter with SQL"
          else s"Filter to $f"
        }
      }
      def enabled = FEStatus.enabled
      override def getOutputs() = {
        params.validate()
        if (input.isProject) {
          val project = input.project
          val before = project.rootEditor.viewer
          if (params("vertex_filter").trim.nonEmpty) {
            val p = project.viewer.editor // Create a copy.
            // Must use low-level method to avoid automatic conversion to Double.
            p.vertexAttributes.updateEntityMap(
              p.vertexAttributes.iterator.toMap + ("!id" -> p.vertexSet.idAttribute))
            val vf = graph_operations.ExecuteSQL.run(
              "select `!id` from vertices where " + params("vertex_filter"),
              p.viewer.getProtoTables.toMap)
            val vertexEmbedding = {
              val op = graph_operations.FilterByTable("!id")
              op(op.vs, p.vertexSet)(op.t, vf).result.identity
            }
            project.pullBack(vertexEmbedding)
          }
          if (params("edge_filter").trim.nonEmpty) {
            val p = project.viewer.editor
            p.edgeAttributes.updateEntityMap(
              p.edgeAttributes.iterator.toMap + ("!id" -> p.edgeBundle.idSet.idAttribute))
            val vf = graph_operations.ExecuteSQL.run(
              "select `!id` from edge_attributes where " + params("edge_filter"),
              p.viewer.getProtoTables.toMap)
            val edgeEmbedding = {
              val op = graph_operations.FilterByTable("!id")
              op(op.vs, p.edgeBundle.idSet)(op.t, vf).result.identity
            }
            project.pullBackEdges(edgeEmbedding)
          }
          updateDeltas(project.rootEditor, before)
          Map(context.box.output("output") -> BoxOutputState.from(project))
        } else {
          assert(input.isTable, "Input must be a graph or a table.")
          Map(context.box.output("output") -> BoxOutputState.from(
            if (params("filter").trim.isEmpty) input.table
            else graph_operations.ExecuteSQL.run(
              "select * from input where " + params("filter"),
              Map("input" -> ProtoTable(input.table)))))
        }
      }
      def apply(): Unit = ???
    },
  )

  registerOp(
    "Transform",
    defaultIcon,
    category,
    List("input"),
    List("table"),
    new TableOutputOperation(_) {
      def paramNames = tableInput("input").schema.fieldNames
      params ++= paramNames.map {
        name => Code(s"new_$name", s"$name", defaultValue = s"$name", language = "sql", enableTableBrowser = false)
      }
      def transformedColumns = paramNames.filter(name => params(name) != name).mkString(", ")
      override def summary = s"Transform $transformedColumns"
      def enabled = FEStatus.enabled
      override def getOutputs() = {
        params.validate()
        val transformations = paramNames.map { name =>
          val newName = s"new_$name"
          s"${params(newName)} as `$name`"
        }.mkString(", ")
        val sql = s"select $transformations from input"
        val protoTables = this.getInputTables()
        val result = graph_operations.ExecuteSQL.run(sql, protoTables)
        makeOutput(result)
      }
    },
  )

  registerOp(
    "Derive column",
    defaultIcon,
    category,
    List("input"),
    List("table"),
    new TableOutputOperation(_) {
      def paramNames = tableInput("input").schema.fieldNames
      params ++= List(
        Param("name", "Column name"),
        Code("value", s"Column value", language = "sql", enableTableBrowser = false))
      def name = params("name")
      def value = params("value")
      override def summary = s"Derive $name = $value"
      def enabled = FEStatus.enabled
      override def getOutputs() = {
        params.validate()
        // SparkSQL allows multiple columns with the same name, so we have to remove it manually.
        val paramsStr = paramNames.filter(_ != name).mkString(", ")
        val sql = s"select $paramsStr, $value as `$name` from input"
        val protoTables = this.getInputTables()
        val result = graph_operations.ExecuteSQL.run(sql, protoTables)
        makeOutput(result)
      }
    },
  )

  registerOp(
    "Compute in Python",
    defaultIcon,
    category,
    List("graph"),
    List("graph"),
    new SmartOperation(_) {
      params ++= List(
        Param("inputs", "Inputs", defaultValue = "<infer from code>"),
        Param("outputs", "Outputs", defaultValue = "<infer from code>"),
        Code("code", "Python code", language = "python"),
      )
      val input = context.inputs("graph")
      private def pythonInputs = {
        if (params("inputs") == "<infer from code>")
          PythonUtilities.inferInputs(params("code"), input.kind)
        else splitParam("inputs")
      }
      private def pythonOutputs = {
        if (params("outputs") == "<infer from code>")
          PythonUtilities.inferOutputs(params("code"), input.kind)
        else splitParam("outputs")
      }
      override def summary = {
        val outputs = pythonOutputs.map(_.replaceFirst(":.*", "")).mkString(", ")
        if (outputs.isEmpty) "Compute in Python"
        else s"Compute $outputs in Python"
      }

      override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
        params.validate()
        PythonUtilities.assertAllowed()
        input.kind match {
          case BoxOutputKind.Project =>
            val project = projectInput("graph")
            if (params("outputs") == "plot") {
              val html = PythonUtilities.deriveHTML(params("code"), pythonInputs, project)
              // The output is called "graph" to preserve compatibility.
              Map(context.box.output("graph") -> BoxOutputState.html(html))
            } else {
              PythonUtilities.derive(params("code"), pythonInputs, pythonOutputs, project)
              Map(context.box.output("graph") -> BoxOutputState.from(project))
            }
          case BoxOutputKind.Table =>
            // We named the input and output before adding table support.
            // It's bad naming here, but lets us keep compatibility.
            val table = tableInput("graph")
            val outputs: Seq[String] =
              if (params("outputs") == "<infer from code>")
                table.schema.fields.map { f =>
                  s"df.${f.name}: " + (f.dataType match {
                    case org.apache.spark.sql.types.StringType => "str"
                    case org.apache.spark.sql.types.LongType => "int"
                    case _ => "float"
                  })
                } ++ pythonOutputs
              else pythonOutputs
            val result = PythonUtilities.deriveTable(params("code"), table, outputs)
            Map(context.box.output("graph") -> BoxOutputState.from(result))
        }
      }
      // Unused because we are overriding getOutputs.
      protected def apply(): Unit = ???
      protected def enabled: com.lynxanalytics.biggraph.controllers.FEStatus = ???
    },
  )

  registerOp(
    "Compute in R",
    defaultIcon,
    category,
    List("input"),
    List("output"),
    new SmartOperation(_) {
      params ++= List(
        Param("inputs", "Inputs", defaultValue = "<infer from code>"),
        Param("outputs", "Outputs", defaultValue = "<infer from code>"),
        Code("code", "R code", language = "r"),
      )
      val input = context.inputs("input")
      private def rInputs = splitParam("inputs")
      private def rOutputs = splitParam("outputs")
      override def summary = {
        val outputs = rOutputs.map(_.replaceFirst(":.*", "")).mkString(", ")
        if (outputs.isEmpty) "Compute in R"
        else s"Compute $outputs in R"
      }

      override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
        params.validate()
        graph_operations.DeriveR.assertAllowed()
        input.kind match {
          case BoxOutputKind.Project =>
            val project = projectInput("input")
            graph_operations.DeriveR.derive(params("code"), rInputs, rOutputs, project)
            Map(context.box.output("output") -> BoxOutputState.from(project))
          case BoxOutputKind.Table =>
            val table = tableInput("input")
            val outputs: Seq[String] =
              table.schema.fields.map { f =>
                s"df.${f.name}: " + (f.dataType match {
                  case org.apache.spark.sql.types.StringType => "character"
                  case org.apache.spark.sql.types.LongType => "integer"
                  case _ => "double"
                })
              } ++ rOutputs
            log.error(s"DeriveR outputs: $outputs")
            val result = graph_operations.DeriveR.deriveTable(params("code"), table, outputs)
            Map(context.box.output("output") -> BoxOutputState.from(result))
        }
      }
      // Unused because we are overriding getOutputs.
      protected def apply(): Unit = ???
      protected def enabled: com.lynxanalytics.biggraph.controllers.FEStatus = ???
    },
  )
}
