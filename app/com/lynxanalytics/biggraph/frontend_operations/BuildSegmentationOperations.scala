// Frontend operations for creating segmentations and / or segmentation links.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers.AttributeWithAggregator
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Attribute
import com.lynxanalytics.biggraph.graph_api.SerializableType

class BuildSegmentationOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  val BuildSegmentationOperations = Category("Build segmentation", "blue")
  val defaultIcon = "hammer"

  def register(id: String)(factory: Context => ProjectTransformation): Unit = {
    registerOp(id, defaultIcon, BuildSegmentationOperations, List(projectOutput), List(projectOutput), factory)
  }

  def register(id: String, inputs: List[String])(factory: Context => ProjectOutputOperation): Unit = {
    registerOp(id, defaultIcon, BuildSegmentationOperations, inputs, List(projectOutput), factory)
  }

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Combine segmentations")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "New segmentation name"),
      Choice(
        "segmentations", "Segmentations",
        options = project.segmentationList, multipleChoice = true))
    def enabled = FEStatus.assert(project.segmentationList.nonEmpty, "No segmentations")
    override def summary = {
      val segmentations = params("segmentations").replace(",", ", ")
      s"Combination of $segmentations"
    }

    def apply() = {
      val segmentations =
        splitParam("segmentations").map(project.existingSegmentation(_))
      assert(segmentations.size >= 2, "Please select at least 2 segmentations to combine.")
      val result = project.segmentation(params("name"))
      // Start by copying the first segmentation.
      val first = segmentations.head
      result.vertexSet = first.vertexSet;
      result.notes = summary
      result.belongsTo = first.belongsTo
      for ((name, attr) <- first.vertexAttributes) {
        result.newVertexAttribute(
          s"${first.segmentationName}_$name", attr)
      }
      // Then combine the other segmentations one by one.
      for (seg <- segmentations.tail) {
        val combination = {
          val op = graph_operations.CombineSegmentations()
          op(op.belongsTo1, result.belongsTo)(op.belongsTo2, seg.belongsTo).result
        }
        val attrs = result.vertexAttributes.toMap
        result.vertexSet = combination.segments
        result.belongsTo = combination.belongsTo
        for ((name, attr) <- attrs) {
          // These names are already prefixed.
          result.vertexAttributes(name) = attr.pullVia(combination.origin1)
        }
        for ((name, attr) <- seg.vertexAttributes) {
          // Add prefix for the new attributes.
          result.newVertexAttribute(
            s"${seg.segmentationName}_$name",
            attr.pullVia(combination.origin2))
        }
      }
      // Calculate sizes and ids at the end.
      result.newVertexAttribute("size", computeSegmentSizes(result))
      result.newVertexAttribute("id", result.vertexSet.idAttribute)
    }
  })

  register("Define segmentation links from matching attributes")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = params ++= List(
      Choice(
        "base_id_attr",
        s"Identifying vertex attribute in base project",
        options = FEOption.list(parent.vertexAttributeNames[String].toList)),
      Choice(
        "seg_id_attr",
        s"Identifying vertex attribute in segmentation",
        options = project.vertexAttrList[String]))
    def enabled =
      project.assertSegmentation &&
        FEStatus.assert(
          project.vertexAttrList[String].nonEmpty,
          "No String vertex attributes in this segmentation.") &&
          FEStatus.assert(
            parent.vertexAttributeNames[String].nonEmpty,
            "No String vertex attributes in base project.")
    def apply() = {
      val baseIdAttr = parent.vertexAttributes(params("base_id_attr")).runtimeSafeCast[String]
      val segIdAttr = project.vertexAttributes(params("seg_id_attr")).runtimeSafeCast[String]
      val op = graph_operations.EdgesFromBipartiteAttributeMatches[String]()
      seg.belongsTo = op(op.fromAttr, baseIdAttr)(op.toAttr, segIdAttr).result.edges
    }
  })

  register("Discard segmentation links")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    def enabled = project.assertSegmentation
    def apply() = {
      val op = graph_operations.EmptyEdgeBundle()
      seg.belongsTo = op(op.src, parent.vertexSet)(op.dst, seg.vertexSet).result.eb
    }
  })

  register("Find connected components")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Segmentation name", defaultValue = "connected_components"),
      Choice(
        "directions",
        "Edge direction",
        options = FEOption.list("ignore directions", "require both directions")))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val directions = params("directions")
      val symmetric = directions match {
        case "ignore directions" => project.edgeBundle.addReversed
        case "require both directions" => project.edgeBundle.makeSymmetric
      }
      val op = graph_operations.ConnectedComponents()
      val result = op(op.es, symmetric).result
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(result.segments, idAttr = "id")
      segmentation.notes = s"Find connected components (edges: $directions)"
      segmentation.belongsTo = result.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))
    }
  })

  register("Find infocom communities")(new ProjectTransformation(_) {
    params ++= List(
      Param(
        "cliques_name", "Name for maximal cliques segmentation", defaultValue = "maximal_cliques"),
      Param(
        "communities_name", "Name for communities segmentation", defaultValue = "communities"),
      Choice("bothdir", "Edges required in cliques in both directions", options = FEOption.bools),
      NonNegInt("min_cliques", "Minimum clique size", default = 3),
      Ratio(
        "adjacency_threshold", "Adjacency threshold for clique overlaps", defaultValue = "0.6"))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val minCliques = params("min_cliques").toInt
      val bothDir = params("bothdir").toBoolean
      val adjacencyThreshold = params("adjacency_threshold").toDouble

      val cliquesResult = {
        val op = graph_operations.FindMaxCliques(minCliques, bothDir)
        op(op.es, project.edgeBundle).result
      }

      val cliquesSegmentation = project.segmentation(params("cliques_name"))
      cliquesSegmentation.setVertexSet(cliquesResult.segments, idAttr = "id")
      cliquesSegmentation.notes =
        s"Find maximal cliques (edges in both directions: $bothDir, minimum clique size: $minCliques)"
      cliquesSegmentation.belongsTo = cliquesResult.belongsTo
      cliquesSegmentation.newVertexAttribute("size", computeSegmentSizes(cliquesSegmentation))

      val cedges = {
        val op = graph_operations.InfocomOverlapForCC(adjacencyThreshold)
        op(op.belongsTo, cliquesResult.belongsTo).result.overlaps
      }

      val ccResult = {
        val op = graph_operations.ConnectedComponents()
        op(op.es, cedges).result
      }

      val weightedVertexToClique = cliquesResult.belongsTo.const(1.0)
      val weightedCliqueToCommunity = ccResult.belongsTo.const(1.0)

      val vertexToCommunity = {
        val op = graph_operations.ConcatenateBundles()
        op(
          op.edgesAB, cliquesResult.belongsTo)(
            op.edgesBC, ccResult.belongsTo)(
              op.weightsAB, weightedVertexToClique)(
                op.weightsBC, weightedCliqueToCommunity).result.edgesAC
      }

      val communitiesSegmentation = project.segmentation(params("communities_name"))
      communitiesSegmentation.setVertexSet(ccResult.segments, idAttr = "id")
      communitiesSegmentation.notes =
        s"Infocom communities (edges in both directions: $bothDir, minimum clique size:" +
          s" $minCliques, adjacency threshold: $adjacencyThreshold)"
      communitiesSegmentation.belongsTo = vertexToCommunity
      communitiesSegmentation.newVertexAttribute(
        "size", computeSegmentSizes(communitiesSegmentation))
    }
  })

  register("Find maximal cliques")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Segmentation name", defaultValue = "maximal_cliques"),
      Choice(
        "bothdir", "Edges required in both directions", options = FEOption.bools),
      NonNegInt("min", "Minimum clique size", default = 3))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val minCliques = params("min").toInt
      val bothDir = params("bothdir").toBoolean
      val op = graph_operations.FindMaxCliques(minCliques, bothDir)
      val result = op(op.es, project.edgeBundle).result
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(result.segments, idAttr = "id")
      segmentation.notes =
        s"Find maximal cliques (edges in both directions: $bothDir, minimum clique size: $minCliques)"
      segmentation.belongsTo = result.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))
    }
  })

  register("Find modular clustering")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Segmentation name", defaultValue = "modular_clusters"),
      Choice("weights", "Weight attribute", options =
        FEOption.noWeight +: project.edgeAttrList[Double]),
      Param(
        "max_iterations",
        "Maximum number of iterations to do",
        defaultValue = "30"),
      Param(
        "min_increment_per_iteration",
        "Minimal modularity increment in an iteration to keep going",
        defaultValue = "0.001"))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val edgeBundle = project.edgeBundle
      val weightsName = params("weights")
      val weights =
        if (weightsName == FEOption.noWeight.id) edgeBundle.const(1.0)
        else project.edgeAttributes(weightsName).runtimeSafeCast[Double]
      val result = {
        val op = graph_operations.FindModularClusteringByTweaks(
          params("max_iterations").toInt, params("min_increment_per_iteration").toDouble)
        op(op.edges, edgeBundle)(op.weights, weights).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(result.clusters, idAttr = "id")
      segmentation.notes =
        if (weightsName == FEOption.noWeight.id) "Modular clustering"
        else s"Modular clustering by $weightsName"
      segmentation.belongsTo = result.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))

      val symmetricDirection = Direction("all edges", project.edgeBundle)
      val symmetricEdges = symmetricDirection.edgeBundle
      val symmetricWeights = symmetricDirection.pull(weights)
      val modularity = {
        val op = graph_operations.Modularity()
        op(op.edges, symmetricEdges)(op.weights, symmetricWeights)(op.belongsTo, result.belongsTo)
          .result.modularity
      }
      segmentation.scalars("modularity") = modularity
    }
  })

  register("Find triangles")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Segmentation name", defaultValue = "triangles"),
      Choice("bothdir", "Edges required in both directions", options = FEOption.bools))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val bothDir = params("bothdir").toBoolean
      val op = graph_operations.EnumerateTriangles(bothDir)
      val result = op(op.vs, project.vertexSet)(op.es, project.edgeBundle).result
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(result.segments, idAttr = "id")
      segmentation.notes =
        s"Finds all triangles in a graph"
      segmentation.belongsTo = result.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))
    }
  })

  register("Use base project as segmentation")(new ProjectTransformation(_) {
    params += Param("name", "Segmentation name", defaultValue = "self_as_segmentation")
    def enabled = project.hasVertexSet

    def apply() = {
      val oldProjectState = project.state
      val segmentation = project.segmentation(params("name"))
      segmentation.state = oldProjectState
      for (subSegmentationName <- segmentation.segmentationNames) {
        segmentation.deleteSegmentation(subSegmentationName)
      }

      val op = graph_operations.LoopEdgeBundle()
      segmentation.belongsTo = op(op.vs, project.vertexSet).result.eb
    }
  })

  register("Use project as segmentation", List("project", "segmentation"))(
    new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val them = projectInput("segmentation")
      params += Param("name", "Segmentation's name", defaultValue = "segmentation")
      def enabled = project.hasVertexSet && them.hasVertexSet
      def apply() = {
        val segmentation = project.segmentation(params("name"))
        segmentation.state = them.state
        val op = graph_operations.EmptyEdgeBundle()
        segmentation.belongsTo = op(op.src, project.vertexSet)(op.dst, them.vertexSet).result.eb
      }
    })

  register(
    "Use table as segmentation", List(projectInput, "segmentation"))(
      new ProjectOutputOperation(_) {
        override lazy val project = projectInput("project")
        lazy val segTable = tableLikeInput("segmentation").asProject
        params ++= List(
          Param("name", s"Name of new segmentation"),
          Choice("base_id_attr", "Vertex ID attribute",
            options = FEOption.unset +: project.vertexAttrList),
          Choice("base_id_column", "Vertex ID column",
            options = FEOption.unset +: segTable.vertexAttrList),
          Choice("seg_id_column", "Segment ID column",
            options = FEOption.unset +: segTable.vertexAttrList))
        def enabled = FEStatus.assert(
          project.vertexAttrList.nonEmpty, "No suitable vertex attributes") &&
          FEStatus.assert(segTable.vertexAttrList.nonEmpty, "No columns in table")
        def apply() = {
          val baseColumnName = params("base_id_column")
          val segColumnName = params("seg_id_column")
          val baseAttrName = params("base_id_attr")
          assert(baseColumnName != FEOption.unset.id,
            "The identifying column parameter must be set for the base project.")
          assert(segColumnName != FEOption.unset.id,
            "The identifying column parameter must be set for the segmentation.")
          assert(baseAttrName != FEOption.unset.id,
            "The base ID attribute parameter must be set.")
          val baseColumn = segTable.vertexAttributes(baseColumnName)
          val segColumn = segTable.vertexAttributes(segColumnName)
          val baseAttr = project.vertexAttributes(baseAttrName)
          val segmentation = project.segmentation(params("name"))

          val segAttr = typedImport(segmentation, baseColumn, segColumn, baseAttr)
          segmentation.newVertexAttribute(segColumnName, segAttr)
        }

        def typedImport[A, B](
          segmentation: SegmentationEditor,
          baseColumn: Attribute[A], segColumn: Attribute[B], baseAttr: Attribute[_]): Attribute[B] = {
          // Merge by segment ID to create the segments.
          val merge = {
            val op = graph_operations.MergeVertices[B]()
            op(op.attr, segColumn).result
          }
          segmentation.setVertexSet(merge.segments, idAttr = "id")
          // Move segment ID to the segments.
          val segAttr = aggregateViaConnection(
            merge.belongsTo,
            // Use scalable aggregator.
            AttributeWithAggregator(segColumn, graph_operations.Aggregator.First[B]()))
          implicit val ta = baseColumn.typeTag
          implicit val tb = segColumn.typeTag
          // Import belongs-to relationship as edges between the base and the segmentation.
          val imp = graph_operations.ImportEdgesForExistingVertices.run(
            baseAttr.runtimeSafeCast[A], segAttr, baseColumn, segColumn)
          segmentation.belongsTo = imp.edges
          segAttr
        }
      })

  register(
    "Use table as segmentation links", List(projectInput, "links"))(new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val links = tableLikeInput("links").asProject
      def seg = project.asSegmentation
      def parent = seg.parent
      if (project.isSegmentation) params ++= List(
        Choice(
          "base_id_attr",
          s"Identifying vertex attribute in base project",
          options = FEOption.unset +: parent.vertexAttrList),
        Choice(
          "base_id_column",
          s"Identifying column for base project",
          options = FEOption.unset +: links.vertexAttrList),
        Choice(
          "seg_id_attr",
          s"Identifying vertex attribute in segmentation",
          options = FEOption.unset +: project.vertexAttrList),
        Choice(
          "seg_id_column",
          s"Identifying column for segmentation",
          options = FEOption.unset +: links.vertexAttrList))
      def enabled =
        project.assertSegmentation &&
          FEStatus.assert(
            project.vertexAttrList.nonEmpty,
            "No vertex attributes in this segmentation") &&
            FEStatus.assert(
              parent.vertexAttributeNames.nonEmpty,
              "No vertex attributes in base project")
      def apply() = {
        val baseColumnName = params("base_id_column")
        val segColumnName = params("seg_id_column")
        val baseAttrName = params("base_id_attr")
        val segAttrName = params("seg_id_attr")
        assert(baseColumnName != FEOption.unset.id,
          "The identifying column parameter must be set for the base project.")
        assert(segColumnName != FEOption.unset.id,
          "The identifying column parameter must be set for the segmentation.")
        assert(baseAttrName != FEOption.unset.id,
          "The base ID attribute parameter must be set.")
        assert(segAttrName != FEOption.unset.id,
          "The segmentation ID attribute parameter must be set.")
        val imp = graph_operations.ImportEdgesForExistingVertices.runtimeSafe(
          parent.vertexAttributes(baseAttrName),
          project.vertexAttributes(segAttrName),
          links.vertexAttributes(baseColumnName),
          links.vertexAttributes(segColumnName))
        seg.belongsTo = imp.edges
      }
    })

  register("Segment by Double attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Segmentation name", defaultValue = "bucketing"),
      Choice("attr", "Attribute", options = project.vertexAttrList[Double]),
      NonNegDouble("interval_size", "Interval size"),
      Choice("overlap", "Overlap", options = FEOption.noyes))
    def enabled = FEStatus.assert(
      project.vertexAttrList[Double].nonEmpty, "No Double vertex attributes.")
    override def summary = {
      val attrName = params("attr")
      val overlap = params("overlap") == "yes"
      val name = params("name")
      s"Segmentation by $attrName" + (if (overlap) " with overlap" else "") + ": $name"
    }

    def apply() = {
      val attrName = params("attr")
      val attr = project.vertexAttributes(attrName).runtimeSafeCast[Double]
      val overlap = params("overlap") == "yes"
      val intervalSize = params("interval_size").toDouble
      val bucketing = {
        val op = graph_operations.DoubleBucketing(intervalSize, overlap)
        op(op.attr, attr).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(bucketing.segments, idAttr = "id")
      segmentation.notes = summary
      segmentation.belongsTo = bucketing.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))
      segmentation.newVertexAttribute("bottom", bucketing.bottom)
      segmentation.newVertexAttribute("top", bucketing.top)
    }
  })

  register("Segment by String attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Segmentation name", defaultValue = "bucketing"),
      Choice("attr", "Attribute", options = project.vertexAttrList[String]))
    def enabled = FEStatus.assert(
      project.vertexAttrList[String].nonEmpty, "No String vertex attributes.")
    override def summary = {
      val attrName = params("attr")
      val name = params("name")
      s"Segmentation by $attrName" + ": $name"
    }

    def apply() = {
      val attrName = params("attr")
      val attr = project.vertexAttributes(attrName).runtimeSafeCast[String]
      val bucketing = {
        val op = graph_operations.StringBucketing()
        op(op.attr, attr).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(bucketing.segments, idAttr = "id")
      segmentation.notes = summary
      segmentation.belongsTo = bucketing.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))
      segmentation.newVertexAttribute(attrName, bucketing.label)
    }
  })

  register("Segment by Vector attribute")(new ProjectTransformation(_) {
    def vectorAttributes =
      project.vertexAttrList[Vector[Double]] ++
        project.vertexAttrList[Vector[String]] ++
        project.vertexAttrList[Vector[Long]]
    params ++= List(
      Param("name", "Segmentation name"),
      Choice("attr", "Attribute", options = vectorAttributes))
    def enabled = FEStatus.assert(vectorAttributes.nonEmpty, "No suitable vector vertex attributes.")
    override def summary = {
      val attrName = params("attr")
      s"Segmentation by $attrName"
    }

    def apply() = {
      import SerializableType.Implicits._
      val attrName = params("attr")
      val attr = project.vertexAttributes(attrName)
      val bucketing = {
        val tt = attr.typeTag
        tt match {
          case _ if tt == scala.reflect.runtime.universe.typeTag[Vector[Double]] =>
            val op = graph_operations.SegmentByVectorAttribute[Double]()(SerializableType.double)
            op(op.attr, attr.runtimeSafeCast[Vector[Double]]).result
          case _ if tt == scala.reflect.runtime.universe.typeTag[Vector[String]] =>
            val op = graph_operations.SegmentByVectorAttribute[String]()(SerializableType.string)
            op(op.attr, attr.runtimeSafeCast[Vector[String]]).result
          case _ if tt == scala.reflect.runtime.universe.typeTag[Vector[Long]] =>
            val op = graph_operations.SegmentByVectorAttribute[Long]()(SerializableType.long)
            op(op.attr, attr.runtimeSafeCast[Vector[Long]]).result
        }
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(bucketing.segments, idAttr = "id")
      segmentation.notes = summary
      segmentation.belongsTo = bucketing.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))
      segmentation.newVertexAttribute(attrName, bucketing.label)
    }
  })

  register("Segment by geographical proximity")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Name"),
      Choice("position", "Position", options = project.vertexAttrList[(Double, Double)]),
      Choice("shapefile", "Shapefile", options = listShapefiles(), allowUnknownOption = true),
      NonNegDouble("distance", "Distance", defaultValue = "0.0"),
      Choice("ignoreUnsupportedShapes", "Ignore unsupported shape types",
        options = FEOption.boolsDefaultFalse))
    def enabled = FEStatus.assert(
      project.vertexAttrList[(Double, Double)].nonEmpty, "No position vertex attributes.")

    def apply() = {
      import com.lynxanalytics.biggraph.graph_util.Shapefile
      val shapeFilePath = getShapeFilePath(params)
      val position = project.vertexAttributes(params("position")).runtimeSafeCast[(Double, Double)]
      val shapefile = Shapefile(shapeFilePath)
      val op = graph_operations.SegmentByGeographicalProximity(
        shapeFilePath,
        params("distance").toDouble,
        shapefile.attrNames,
        params("ignoreUnsupportedShapes").toBoolean)
      val result = op(op.coordinates, position).result
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(result.segments, idAttr = "id")
      segmentation.notes = summary
      segmentation.belongsTo = result.belongsTo

      for ((attrName, i) <- shapefile.attrNames.zipWithIndex) {
        segmentation.newVertexAttribute(attrName, result.attributes(i))
      }
      shapefile.close()
    }
  })

  register("Segment by interval")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Segmentation name", defaultValue = "bucketing"),
      Choice("begin_attr", "Begin attribute", options = project.vertexAttrList[Double]),
      Choice("end_attr", "End attribute", options = project.vertexAttrList[Double]),
      NonNegDouble("interval_size", "Interval size"),
      Choice("overlap", "Overlap", options = FEOption.noyes))
    def enabled = FEStatus.assert(
      project.vertexAttrList[Double].size >= 2,
      "Less than two Double vertex attributes.")
    override def summary = {
      val beginAttrName = params("begin_attr")
      val endAttrName = params("end_attr")
      val overlap = params("overlap") == "yes"
      val name = params("name")
      s"Interval segmentation by $beginAttrName and $endAttrName" + (if (overlap) " with overlap" else "") + ": $name"
    }

    def apply() = {
      val beginAttrName = params("begin_attr")
      val endAttrName = params("end_attr")
      val beginAttr = project.vertexAttributes(beginAttrName).runtimeSafeCast[Double]
      val endAttr = project.vertexAttributes(endAttrName).runtimeSafeCast[Double]
      val overlap = params("overlap") == "yes"
      val intervalSize = params("interval_size").toDouble
      val bucketing = {
        val op = graph_operations.IntervalBucketing(intervalSize, overlap)
        op(op.beginAttr, beginAttr)(op.endAttr, endAttr).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.setVertexSet(bucketing.segments, idAttr = "id")
      segmentation.notes = summary
      segmentation.belongsTo = bucketing.belongsTo
      segmentation.newVertexAttribute("size", computeSegmentSizes(segmentation))
      segmentation.newVertexAttribute("bottom", bucketing.bottom)
      segmentation.newVertexAttribute("top", bucketing.top)
    }
  })

  register("Segment by event sequence")(new ProjectTransformation(_) {
    val SegmentationPrefix = "Segmentation: "
    val AttributePrefix = "Attribute: "
    val possibleLocations =
      project
        .segmentations
        .map { seg => FEOption.regular(SegmentationPrefix + seg.segmentationName) }
        .toList ++
        project.vertexAttrList[String].map {
          attr => FEOption.regular(AttributePrefix + attr.title)
        }

    params ++= List(
      Param("name", "Target segmentation name"),
      Choice(
        "location",
        "Location",
        options = possibleLocations),
      Choice("time_attr", "Time attribute", options = project.vertexAttrList[Double]),
      Choice("algorithm", "Algorithm", options = List(
        FEOption("continuous", "Take continuous event sequences"),
        FEOption("with-gaps", "Allow gaps in event sequences"))),
      NonNegInt("sequence_length", "Sequence length", default = 2),
      NonNegDouble("time_window_step", "Time window step"),
      NonNegDouble("time_window_length", "Time window length"))

    def enabled =
      FEStatus.assert(project.isSegmentation, "Must be run on a segmentation") &&
        FEStatus.assert(
          possibleLocations.nonEmpty,
          "There must be a String attribute or a sub-segmentation to define event locations") &&
          FEStatus.assert(
            project.vertexAttrList[Double].nonEmpty,
            "There must be a Double attribute to define event times")

    override def summary = {
      val name = params("name")
      s"Segmentation by event sequence: $name"
    }

    def apply() = {
      val timeAttrName = params("time_attr")
      val timeAttr = project.vertexAttributes(timeAttrName).runtimeSafeCast[Double]
      val locationAttr = params("location")
      val belongsToLocation =
        if (locationAttr.startsWith(SegmentationPrefix)) {
          project.existingSegmentation(locationAttr.substring(SegmentationPrefix.length)).belongsTo
        } else {
          val locationAttribute =
            project.vertexAttributes(locationAttr.substring(AttributePrefix.length)).runtimeSafeCast[String]
          val op = graph_operations.StringBucketing()
          op(op.attr, locationAttribute).result.belongsTo.entity
        }

      val cells = {
        val op = graph_operations.SegmentByEventSequence(
          params("algorithm"),
          params("sequence_length").toInt,
          params("time_window_step").toDouble,
          params("time_window_length").toDouble)
        op(op.personBelongsToEvent, project.asSegmentation.belongsTo)(
          op.eventTimeAttribute, timeAttr)(
            op.eventBelongsToLocation, belongsToLocation).result
      }
      val segmentation = project.asSegmentation.parent.segmentation(params("name"))
      segmentation.setVertexSet(cells.segments, idAttr = "id")
      segmentation.notes = summary
      segmentation.belongsTo = cells.belongsTo
      segmentation.vertexAttributes("description") = cells.segmentDescription
      segmentation.vertexAttributes("size") = toDouble(cells.segmentSize)
    }
  })
}
