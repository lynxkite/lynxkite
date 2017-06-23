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
    new ExportOperations(env),
    new PlotOperations(env),
    new VisualizationOperations(env))

  override val atomicOperations = registries.map(_.operations).flatten.toMap
  override val atomicCategories = registries.map(_.categories).flatten.toMap
}

class ProjectOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  private val projectInput = "project" // The default input name, just to avoid typos.
  private val projectOutput = "project"
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
  val SpecialtyOperations = Category("Specialty operations", "green", icon = "book")
  val EdgeAttributesOperations =
    Category("Edge attribute operations", "blue", sortKey = "Attribute, edge")
  val VertexAttributesOperations =
    Category("Vertex attribute operations", "blue", sortKey = "Attribute, vertex")
  val GlobalOperations = Category("Global operations", "magenta", icon = "globe")
  val ImportOperations = Category("Import operations", "yellow", icon = "import")
  val MetricsOperations = Category("Graph metrics", "green", icon = "stats")
  val PropagationOperations = Category("Propagation operations", "green", icon = "fullscreen")
  val HiddenOperations = Category("Hidden operations", "black", visible = false)
  val DeprecatedOperations =
    Category("Deprecated operations", "red", visible = false, icon = "remove-sign")
  val CreateSegmentationOperations = Category("Create segmentation", "green", icon = "th-large")
  val StructureOperations = Category("Structure operations", "pink", icon = "asterisk")
  val MachineLearningOperations = Category("Machine learning operations", "pink ", icon = "knight")
  val UtilityOperations = Category("Utility operations", "green", icon = "wrench", sortKey = "zz")

  import OperationParams._

  register("Discard vertices", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasVertexSet && project.assertNotSegmentation
    def apply() = {
      project.vertexSet = null
    }
  })

  register("Discard edges", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      project.edgeBundle = null
    }
  })

  register("Discard segmentation links", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    def enabled = project.assertSegmentation
    def apply() = {
      val op = graph_operations.EmptyEdgeBundle()
      seg.belongsTo = op(op.src, parent.vertexSet)(op.dst, seg.vertexSet).result.eb
    }
  })

  register("Create vertices", StructureOperations)(new ProjectOutputOperation(_) {
    params += NonNegInt("size", "Vertex set size", default = 10)
    def enabled = FEStatus.enabled
    def apply() = {
      val result = graph_operations.CreateVertexSet(params("size").toLong)().result
      project.setVertexSet(result.vs, idAttr = "id")
      project.newVertexAttribute("ordinal", result.ordinal)
    }
  })

  register("Create random edge bundle", StructureOperations, new ProjectTransformation(_) {
    params ++= List(
      NonNegDouble("degree", "Average degree", defaultValue = "10.0"),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet
    def apply() = {
      val op = graph_operations.FastRandomEdgeBundle(
        params("seed").toInt, params("degree").toDouble)
      project.edgeBundle = op(op.vs, project.vertexSet).result.es
    }
  })

  register("Create scale-free random edge bundle", StructureOperations, new ProjectTransformation(_) {
    params ++= List(
      NonNegInt("iterations", "Number of iterations", default = 10),
      NonNegDouble(
        "perIterationMultiplier",
        "Per iteration edge number multiplier",
        defaultValue = "1.3"),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet
    def apply() = {
      val op = graph_operations.ScaleFreeEdgeBundle(
        params("iterations").toInt,
        params("seed").toLong,
        params("perIterationMultiplier").toDouble)
      project.edgeBundle = op(op.vs, project.vertexSet).result.es
    }
  })

  register("Connect vertices on attribute", StructureOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("fromAttr", "Source attribute", options = project.vertexAttrList),
      Choice("toAttr", "Destination attribute", options = project.vertexAttrList))
    def enabled =
      (project.hasVertexSet
        && FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes."))
    private def applyAA[A](fromAttr: Attribute[A], toAttr: Attribute[A]) = {
      if (fromAttr == toAttr) {
        // Use the slightly faster operation.
        val op = graph_operations.EdgesFromAttributeMatches[A]()
        project.edgeBundle = op(op.attr, fromAttr).result.edges
      } else {
        val op = graph_operations.EdgesFromBipartiteAttributeMatches[A]()
        project.edgeBundle = op(op.fromAttr, fromAttr)(op.toAttr, toAttr).result.edges
      }
    }
    private def applyAB[A, B](fromAttr: Attribute[A], toAttr: Attribute[B]) = {
      applyAA(fromAttr, toAttr.asInstanceOf[Attribute[A]])
    }
    def apply() = {
      val fromAttrName = params("fromAttr")
      val toAttrName = params("toAttr")
      val fromAttr = project.vertexAttributes(fromAttrName)
      val toAttr = project.vertexAttributes(toAttrName)
      assert(fromAttr.typeTag.tpe =:= toAttr.typeTag.tpe,
        s"$fromAttrName and $toAttrName are not of the same type.")
      applyAB(fromAttr, toAttr)
    }
  })

  registerOp(
    "Import vertices", defaultIcon, ImportOperations,
    inputs = List("vertices"), outputs = List(projectOutput),
    factory = new ProjectOutputOperation(_) {
      lazy val vertices = tableLikeInput("vertices").asProject
      params += Param("id_attr", "Save internal ID as", defaultValue = "")
      def enabled = FEStatus.enabled
      def apply() = {
        project.vertexSet = vertices.vertexSet
        for ((name, attr) <- vertices.vertexAttributes) {
          project.newVertexAttribute(name, attr, "imported")
        }
        val idAttr = params("id_attr")
        if (idAttr.nonEmpty) {
          assert(
            !project.vertexAttributes.contains(idAttr),
            s"The input also contains a column called '$idAttr'. Please pick a different name.")
          project.newVertexAttribute(idAttr, project.vertexSet.idAttribute, "internal")
        }
      }
    })

  registerOp(
    "Import edges for existing vertices", defaultIcon, ImportOperations,
    inputs = List(projectInput, "edges"), outputs = List(projectOutput),
    factory = new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val edges = tableLikeInput("edges").asProject
      params ++= List(
        Choice("attr", "Vertex ID attribute", options = FEOption.unset +: project.vertexAttrList),
        Choice("src", "Source ID column", options = FEOption.unset +: edges.vertexAttrList),
        Choice("dst", "Destination ID column", options = FEOption.unset +: edges.vertexAttrList))
      def enabled =
        FEStatus.assert(
          project.vertexAttrList.nonEmpty, "No attributes on the project to use as id.") &&
          FEStatus.assert(
            edges.vertexAttrList.nonEmpty, "No attributes on the edges to use as id.")
      def apply() = {
        val src = params("src")
        val dst = params("dst")
        val id = params("attr")
        assert(src != FEOption.unset.id, "The Source ID column parameter must be set.")
        assert(dst != FEOption.unset.id, "The Destination ID column parameter must be set.")
        assert(id != FEOption.unset.id, "The Vertex ID attribute parameter must be set.")
        val idAttr = project.vertexAttributes(id)
        val srcAttr = edges.vertexAttributes(src)
        val dstAttr = edges.vertexAttributes(dst)
        val imp = graph_operations.ImportEdgesForExistingVertices.runtimeSafe(
          idAttr, idAttr, srcAttr, dstAttr)
        project.edgeBundle = imp.edges
        for ((name, attr) <- edges.vertexAttributes) {
          project.edgeAttributes(name) = attr.pullVia(imp.embedding)
        }
      }
    })

  registerOp(
    "Import vertices and edges from a single table", defaultIcon, ImportOperations,
    inputs = List("edges"), outputs = List(projectOutput),
    factory = new ProjectOutputOperation(_) {
      lazy val edges = tableLikeInput("edges").asProject
      params ++= List(
        Choice("src", "Source ID column", options = FEOption.unset +: edges.vertexAttrList),
        Choice("dst", "Destination ID column", options = FEOption.unset +: edges.vertexAttrList))
      def enabled = FEStatus.enabled
      def apply() = {
        val src = params("src")
        val dst = params("dst")
        assert(src != FEOption.unset.id, "The Source ID column parameter must be set.")
        assert(dst != FEOption.unset.id, "The Destination ID column parameter must be set.")
        val eg = {
          val op = graph_operations.VerticesToEdges()
          op(op.srcAttr, edges.vertexAttributes(src).runtimeSafeCast[String])(
            op.dstAttr, edges.vertexAttributes(dst).runtimeSafeCast[String]).result
        }
        project.setVertexSet(eg.vs, idAttr = "id")
        project.newVertexAttribute("stringId", eg.stringId)
        project.edgeBundle = eg.es
        for ((name, attr) <- edges.vertexAttributes) {
          project.edgeAttributes(name) = attr.pullVia(eg.embedding)
        }
      }
    })

  registerOp(
    "Import vertex attributes", defaultIcon, ImportOperations,
    inputs = List(projectInput, "attributes"),
    outputs = List(projectOutput),
    factory = new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val attributes = tableLikeInput("attributes").asProject
      params ++= List(
        Choice("id_attr", "Vertex attribute",
          options = FEOption.unset +: project.vertexAttrList[String]),
        Choice("id_column", "ID column",
          options = FEOption.unset +: attributes.vertexAttrList[String]),
        Param("prefix", "Name prefix for the imported vertex attributes"),
        Choice("unique_keys", "Assert unique vertex attribute values", options = FEOption.bools))
      def enabled =
        project.hasVertexSet &&
          FEStatus.assert(project.vertexAttrList[String].nonEmpty, "No vertex attributes to use as key.")
      def apply() = {
        val idAttrName = params("id_attr")
        val idColumnName = params("id_column")
        assert(idAttrName != FEOption.unset.id, "The vertex attribute parameter must be set.")
        assert(idColumnName != FEOption.unset.id, "The ID column parameter must be set.")
        val idAttr = project.vertexAttributes(idAttrName).runtimeSafeCast[String]
        val idColumn = attributes.vertexAttributes(idColumnName).runtimeSafeCast[String]
        val projectAttrNames = project.vertexAttributeNames
        val uniqueKeys = params("unique_keys").toBoolean
        val edges = if (uniqueKeys) {
          val op = graph_operations.EdgesFromUniqueBipartiteAttributeMatches()
          op(op.fromAttr, idAttr)(op.toAttr, idColumn).result.edges
        } else {
          val op = graph_operations.EdgesFromLookupAttributeMatches()
          op(op.fromAttr, idAttr)(op.toAttr, idColumn).result.edges
        }
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        for ((name, attr) <- attributes.vertexAttributes) {
          assert(!projectAttrNames.contains(prefix + name),
            s"Cannot import column `${prefix + name}`. Attribute already exists.")
          project.newVertexAttribute(prefix + name, attr.pullVia(edges), "imported")
        }
      }
    })

  registerOp(
    "Import edge attributes", defaultIcon, ImportOperations,
    inputs = List(projectInput, "attributes"),
    outputs = List(projectOutput),
    factory = new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val attributes = tableLikeInput("attributes").asProject
      params ++= List(
        Choice("id_attr", "Edge attribute",
          options = FEOption.unset +: project.edgeAttrList[String]),
        Choice("id_column", "ID column", options = FEOption.unset +: attributes.vertexAttrList),
        Param("prefix", "Name prefix for the imported edge attributes"),
        Choice("unique_keys", "Assert unique edge attribute values", options = FEOption.bools))
      def enabled =
        project.hasEdgeBundle &&
          FEStatus.assert(project.edgeAttrList[String].nonEmpty, "No edge attributes to use as key.")
      def apply() = {
        val columnName = params("id_column")
        assert(columnName != FEOption.unset.id, "The ID column parameter must be set.")
        val attrName = params("id_attr")
        assert(attrName != FEOption.unset.id, "The edge attribute parameter must be set.")
        val idAttr = project.edgeAttributes(attrName).runtimeSafeCast[String]
        val idColumn = attributes.vertexAttributes(columnName).runtimeSafeCast[String]
        val projectAttrNames = project.edgeAttributeNames
        val uniqueKeys = params("unique_keys").toBoolean
        val edges = if (uniqueKeys) {
          val op = graph_operations.EdgesFromUniqueBipartiteAttributeMatches()
          op(op.fromAttr, idAttr)(op.toAttr, idColumn).result.edges
        } else {
          val op = graph_operations.EdgesFromLookupAttributeMatches()
          op(op.fromAttr, idAttr)(op.toAttr, idColumn).result.edges
        }
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        for ((name, attr) <- attributes.vertexAttributes) {
          assert(!projectAttrNames.contains(prefix + name),
            s"Cannot import column `${prefix + name}`. Attribute already exists.")
          project.newEdgeAttribute(prefix + name, attr.pullVia(edges), "imported")
        }
      }
    })

  register("Find maximal cliques", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Find connected components", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Find infocom communities", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Find modular clustering", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Segment by Double attribute", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Segment by String attribute", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Segment by Vector attribute", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Segment by interval", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Segment by event sequence", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Find triangles", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Combine segmentations", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Expose internal vertex ID", VertexAttributesOperations, new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "id")
    def enabled = project.hasVertexSet
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      project.newVertexAttribute(params("name"), project.vertexSet.idAttribute, help)
    }
  })

  register("Expose internal edge ID", EdgeAttributesOperations, new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "id")
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      project.newEdgeAttribute(params("name"), project.edgeBundle.idSet.idAttribute, help)
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

  register("Add random vertex attribute", VertexAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "random"),
      Choice("dist", "Distribution", options = FEOption.list(graph_operations.RandomDistribution.getNames)),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet
    override def summary = {
      val dist = params("dist").toLowerCase
      s"Add $dist vertex attribute"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.AddRandomAttribute(params("seed").toInt, params("dist"))
      project.newVertexAttribute(
        params("name"), op(op.vs, project.vertexSet).result.attr, help)
    }
  })

  register("Add random edge attribute", EdgeAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "random"),
      Choice("dist", "Distribution", options = FEOption.list(graph_operations.RandomDistribution.getNames)),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasEdgeBundle
    override def summary = {
      val dist = params("dist").toLowerCase
      s"Add $dist edge attribute"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.AddRandomAttribute(params("seed").toInt, params("dist"))
      project.newEdgeAttribute(
        params("name"), op(op.vs, project.edgeBundle.idSet).result.attr, help)
    }
  })

  register("Add constant edge attribute", EdgeAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"),
      Choice("type", "Type", options = FEOption.list("Double", "String")))
    def enabled = project.hasEdgeBundle
    override def summary = {
      val name = params("name")
      val value = params("value")
      s"Add constant edge attribute: $name = $value"
    }
    def apply() = {
      val value = params("value")
      val res = {
        if (params("type") == "Double") {
          project.edgeBundle.const(value.toDouble)
        } else {
          project.edgeBundle.const(value)
        }
      }
      project.newEdgeAttribute(params("name"), res, s"constant $value")
    }
  })

  register("Add constant vertex attribute", VertexAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name"),
      Param("value", "Value", defaultValue = "1"),
      Choice("type", "Type", options = FEOption.list("Double", "String")))
    def enabled = project.hasVertexSet
    override def summary = {
      val name = params("name")
      val value = params("value")
      s"Add constant vertex attribute: $name = $value"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val value = params("value")
      val op: graph_operations.AddConstantAttribute[_] =
        graph_operations.AddConstantAttribute.doubleOrString(
          isDouble = (params("type") == "Double"), value)
      project.newVertexAttribute(
        params("name"), op(op.vs, project.vertexSet).result.attr, s"constant $value")
    }
  })

  register(
    "Fill vertex attribute with constant default value",
    VertexAttributesOperations, new ProjectTransformation(_) {
      params ++= List(
        Choice(
          "attr", "Vertex attribute",
          options = project.vertexAttrList[String] ++ project.vertexAttrList[Double]),
        Param("def", "Default value"))
      def enabled = FEStatus.assert(
        (project.vertexAttrList[String] ++ project.vertexAttrList[Double]).nonEmpty,
        "No vertex attributes.")
      override def summary = {
        val name = params("attr")
        s"Fill vertex attribute '$name' with constant default value"
      }
      def apply() = {
        val attr = project.vertexAttributes(params("attr"))
        val paramDef = params("def")
        val op: graph_operations.AddConstantAttribute[_] =
          graph_operations.AddConstantAttribute.doubleOrString(
            isDouble = attr.is[Double], paramDef)
        val default = op(op.vs, project.vertexSet).result
        project.newVertexAttribute(
          params("attr"), unifyAttribute(attr, default.attr.entity),
          project.viewer.getVertexAttributeNote(params("attr")) + s" (filled with default $paramDef)" + help)
      }
    })

  register(
    "Fill edge attribute with constant default value",
    EdgeAttributesOperations, new ProjectTransformation(_) {
      params ++= List(
        Choice(
          "attr", "Edge attribute",
          options = project.edgeAttrList[String] ++ project.edgeAttrList[Double]),
        Param("def", "Default value"))
      def enabled = FEStatus.assert(
        (project.edgeAttrList[String] ++ project.edgeAttrList[Double]).nonEmpty,
        "No edge attributes.")
      override def summary = {
        val name = params("attr")
        s"Fill edge attribute '$name' with constant default value"
      }
      def apply() = {
        val attr = project.edgeAttributes(params("attr"))
        val paramDef = params("def")
        val op: graph_operations.AddConstantAttribute[_] =
          graph_operations.AddConstantAttribute.doubleOrString(
            isDouble = attr.is[Double], paramDef)
        val default = op(op.vs, project.edgeBundle.idSet).result
        project.newEdgeAttribute(
          params("attr"), unifyAttribute(attr, default.attr.entity),
          project.viewer.getEdgeAttributeNote(params("attr")) + s" (filled with default $paramDef)" + help)
      }
    })

  register("Merge two vertex attributes", VertexAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "New attribute name", defaultValue = ""),
      Choice("attr1", "Primary attribute", options = project.vertexAttrList),
      Choice("attr2", "Secondary attribute", options = project.vertexAttrList))
    def enabled = FEStatus.assert(
      project.vertexAttrList.size >= 2, "Not enough vertex attributes.")
    override def summary = {
      val name1 = params("attr1")
      val name2 = params("attr2")
      s"Merge two vertex attributes: $name1, $name2"
    }
    def apply() = {
      val name = params("name")
      assert(name.nonEmpty, "You must specify a name for the new attribute.")
      val attr1 = project.vertexAttributes(params("attr1"))
      val attr2 = project.vertexAttributes(params("attr2"))
      assert(attr1.typeTag.tpe =:= attr2.typeTag.tpe,
        "The two attributes must have the same type.")
      project.newVertexAttribute(name, unifyAttribute(attr1, attr2), s"primary: $attr1, secondary: $attr2" + help)
    }
  })

  register("Merge two edge attributes", EdgeAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "New attribute name", defaultValue = ""),
      Choice("attr1", "Primary attribute", options = project.edgeAttrList),
      Choice("attr2", "Secondary attribute", options = project.edgeAttrList))
    def enabled = FEStatus.assert(
      project.edgeAttrList.size >= 2, "Not enough edge attributes.")
    override def summary = {
      val name1 = params("attr1")
      val name2 = params("attr2")
      s"Merge two edge attributes: $name1, $name2"
    }
    def apply() = {
      val name = params("name")
      assert(name.nonEmpty, "You must specify a name for the new attribute.")
      val attr1 = project.edgeAttributes(params("attr1"))
      val attr2 = project.edgeAttributes(params("attr2"))
      assert(attr1.typeTag.tpe =:= attr2.typeTag.tpe,
        "The two attributes must have the same type.")
      project.newEdgeAttribute(name, unifyAttribute(attr1, attr2), s"primary: $attr1, secondary: $attr2" + help)
    }
  })

  register(
    "Reduce vertex attributes to two dimensions",
    MachineLearningOperations, new ProjectTransformation(_) {
      params ++= List(
        Param("output_name1", "First dimension name", defaultValue = "reduced_dimension1"),
        Param("output_name2", "Second dimension name", defaultValue = "reduced_dimension2"),
        Choice(
          "features", "Attributes",
          options = project.vertexAttrList[Double], multipleChoice = true))
      def enabled = FEStatus.assert(
        project.vertexAttrList[Double].size >= 2, "Less than two vertex attributes.")
      def apply() = {
        val featureNames = splitParam("features").sorted
        assert(featureNames.size >= 2, "Please select at least two attributes.")
        val features = featureNames.map {
          name => project.vertexAttributes(name).runtimeSafeCast[Double]
        }
        val op = graph_operations.ReduceDimensions(features.size)
        val result = op(op.features, features).result
        project.newVertexAttribute(params("output_name1"), result.attr1, help)
        project.newVertexAttribute(params("output_name2"), result.attr2, help)
      }
    })

  register("Reverse edge direction", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, project.edgeBundle).result
      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        res.esBA,
        res.injection)
    }
  })

  register("Add reversed edges", StructureOperations, new ProjectTransformation(_) {
    params += Param("distattr", "Distinguishing edge attribute")
    def enabled = project.hasEdgeBundle
    def apply() = {
      val addIsNewAttr = params("distattr").nonEmpty

      val rev = {
        val op = graph_operations.AddReversedEdges(addIsNewAttr)
        op(op.es, project.edgeBundle).result
      }

      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        rev.esPlus,
        rev.newToOriginal)
      if (addIsNewAttr) {
        project.edgeAttributes(params("distattr")) = rev.isNew
      }
    }
  })

  register("Find vertex coloring", MetricsOperations, new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "color")
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.Coloring()
      project.newVertexAttribute(
        params("name"), op(op.es, project.edgeBundle).result.coloring, help)
    }
  })

  register("Compute clustering coefficient", MetricsOperations, new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "clustering_coefficient")
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.ClusteringCoefficient()
      project.newVertexAttribute(
        params("name"), op(op.es, project.edgeBundle).result.clustering, help)
    }
  })

  register("Approximate clustering coefficient", MetricsOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "clustering_coefficient"),
      NonNegInt("bits", "Precision", default = 8))
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.ApproxClusteringCoefficient(params("bits").toInt)
      project.newVertexAttribute(
        params("name"), op(op.es, project.edgeBundle).result.clustering, help)
    }
  })

  register("Compute embeddedness", MetricsOperations, new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "embeddedness")
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.Embeddedness()
      project.newEdgeAttribute(params("name"), op(op.es, project.edgeBundle).result.embeddedness, help)
    }
  })

  register("Approximate embeddedness", MetricsOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "embeddedness"),
      NonNegInt("bits", "Precision", default = 8))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.ApproxEmbeddedness(params("bits").toInt)
      project.newEdgeAttribute(params("name"), op(op.es, project.edgeBundle).result.embeddedness, help)
    }
  })

  register("Compute dispersion", MetricsOperations, new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "dispersion")
    def enabled = project.hasEdgeBundle
    def apply() = {
      val dispersion = {
        val op = graph_operations.Dispersion()
        op(op.es, project.edgeBundle).result.dispersion.entity
      }
      val embeddedness = {
        val op = graph_operations.Embeddedness()
        op(op.es, project.edgeBundle).result.embeddedness.entity
      }
      // http://arxiv.org/pdf/1310.6753v1.pdf
      val normalizedDispersion = {
        val op = graph_operations.DeriveJSDouble(
          JavaScript("Math.pow(disp, 0.61) / (emb + 5)"),
          Seq("disp", "emb"))
        op(op.attrs, graph_operations.VertexAttributeToJSValue.seq(
          dispersion, embeddedness)).result.attr.entity
      }
      // TODO: recursive dispersion
      project.newEdgeAttribute(params("name"), dispersion, help)
      project.newEdgeAttribute("normalized_" + params("name"), normalizedDispersion, help)
    }
  })

  register("Compute degree", MetricsOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "degree"),
      Choice("direction", "Count", options = Direction.options))
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val es = Direction(params("direction"), project.edgeBundle, reversed = true).edgeBundle
      val op = graph_operations.OutDegree()
      project.newVertexAttribute(
        params("name"), op(op.es, es).result.outDegree, params("direction") + help)
    }
  })

  register("Compute PageRank", MetricsOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "page_rank"),
      Choice("weights", "Weight attribute",
        options = FEOption.noWeight +: project.edgeAttrList[Double]),
      NonNegInt("iterations", "Number of iterations", default = 5),
      Ratio("damping", "Damping factor", defaultValue = "0.85"),
      Choice("direction", "Direction",
        options = Direction.attrOptionsWithDefault("outgoing edges")))
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.PageRank(params("damping").toDouble, params("iterations").toInt)
      val weightsName = params("weights")
      val direction = Direction(params("direction"),
        project.edgeBundle, reversed = true)
      val es = direction.edgeBundle
      val weights =
        if (weightsName == FEOption.noWeight.id) es.const(1.0)
        else direction.pull(project.edgeAttributes(params("weights"))).runtimeSafeCast[Double]
      project.newVertexAttribute(
        params("name"), op(op.es, es)(op.weights, weights).result.pagerank, help)
    }
  })

  register("Find shortest path", MetricsOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "shortest_distance"),
      Choice("edge_distance", "Edge distance attribute",
        options = FEOption.unitDistances +: project.edgeAttrList[Double]),
      Choice(
        "starting_distance", "Starting distance attribute",
        options = project.vertexAttrList[Double]),
      NonNegInt("iterations", "Maximum number of iterations", default = 10))
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val startingDistanceAttr = params("starting_distance")
      val startingDistance = project
        .vertexAttributes(startingDistanceAttr)
        .runtimeSafeCast[Double]
      val op = graph_operations.ShortestPath(params("iterations").toInt)
      val edgeDistance =
        if (params("edge_distance") == FEOption.unitDistances.id) {
          project.edgeBundle.const(1.0)
        } else {
          project.edgeAttributes(params("edge_distance")).runtimeSafeCast[Double]
        }
      project.newVertexAttribute(
        params("name"),
        op(op.es, project.edgeBundle)(op.edgeDistance, edgeDistance)(op.startingDistance, startingDistance).result.distance, help)
    }
  })

  register("Compute centrality", MetricsOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "centrality"),
      NonNegInt("maxDiameter", "Maximal diameter to check", default = 10),
      Choice("algorithm", "Centrality type",
        options = FEOption.list("Harmonic", "Lin", "Average distance")),
      NonNegInt("bits", "Precision", default = 8),
      Choice("direction", "Direction",
        options = Direction.attrOptionsWithDefault("outgoing edges")))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val name = params("name")
      val algorithm = params("algorithm")
      assert(name.nonEmpty, "Please set an attribute name.")
      val es = Direction(params("direction"),
        project.edgeBundle, reversed = true).edgeBundle
      val op = graph_operations.HyperBallCentrality(
        params("maxDiameter").toInt, algorithm, params("bits").toInt)
      project.newVertexAttribute(
        name, op(op.es, es).result.centrality, algorithm + help)
    }
  })

  register("Add rank attribute", VertexAttributesOperations, new ProjectTransformation(_) {
    def attrs = (
      project.vertexAttrList[String] ++
      project.vertexAttrList[Double] ++
      project.vertexAttrList[Long] ++
      project.vertexAttrList[Int]).sortBy(_.title)
    params ++= List(
      Param("rankattr", "Rank attribute name", defaultValue = "ranking"),
      Choice("keyattr", "Key attribute name", options = attrs),
      Choice("order", "Order", options = FEOption.list("ascending", "descending")))

    def enabled = FEStatus.assert(attrs.nonEmpty, "No sortable vertex attributes")
    override def summary = {
      val name = params("keyattr")
      s"Add rank attribute for '$name'"
    }
    def apply() = {
      val keyAttr = params("keyattr")
      val rankAttr = params("rankattr")
      val ascending = params("order") == "ascending"
      assert(rankAttr.nonEmpty, "Please set a name for the rank attribute")
      val sortKey = project.vertexAttributes(keyAttr)
      val rank = graph_operations.AddRankingAttribute.run(sortKey, ascending)
      project.newVertexAttribute(rankAttr, rank.asDouble, s"rank by $keyAttr" + help)
    }
  })

  register("Create example graph", StructureOperations)(new ProjectOutputOperation(_) {
    def enabled = FEStatus.enabled
    def apply() = {
      val g = graph_operations.ExampleGraph()().result
      project.vertexSet = g.vertices
      project.edgeBundle = g.edges
      for ((name, attr) <- g.vertexAttributes) {
        project.newVertexAttribute(name, attr)
      }
      project.newVertexAttribute("id", project.vertexSet.idAttribute)
      project.edgeAttributes = g.edgeAttributes.mapValues(_.entity)
      for ((name, s) <- g.scalars) {
        project.scalars(name) = s.entity
      }
      project.setElementMetadata(VertexAttributeKind, "income", MetadataNames.Icon, "money_bag")
      project.setElementMetadata(VertexAttributeKind, "location", MetadataNames.Icon, "paw_prints")
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

  registerOp("Load snapshot", "black_medium_square", StructureOperations,
    inputs = List(), outputs = List("state"), factory = new SimpleOperation(_) {
      params += Param("path", "Path")
      override def getOutputs() = {
        val snapshot = DirectoryEntry.fromName(params("path")).asSnapshotFrame
        Map(context.box.output("state") -> snapshot.getState)
      }
    })

  register("Hash vertex attribute", VertexAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("attr", "Vertex attribute", options = project.vertexAttrList, multipleChoice = true),
      Param("salt", "Salt",
        defaultValue = graph_operations.HashVertexAttribute.makeSecret(nextString(15))))
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes.")

    def apply() = {
      assert(params("attr").nonEmpty, "Please choose at least one vertex attribute to hash.")
      val salt = params("salt")
      graph_operations.HashVertexAttribute.assertSecret(salt)
      assert(
        graph_operations.HashVertexAttribute.getSecret(salt).nonEmpty, "Please set a salt value.")
      val op = graph_operations.HashVertexAttribute(salt)
      for (attribute <- splitParam("attr")) {
        val attr = project.vertexAttributes(attribute).asString
        project.newVertexAttribute(
          attribute, op(op.vs, project.vertexSet)(op.attr, attr).result.hashed, "hashed")
      }
    }

    // To have a more secure default salt value than just using Random.nextInt.toString
    def nextString(length: Int): String = {
      val validCharacters: Array[Char] = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toArray
      val srand: java.security.SecureRandom = new java.security.SecureRandom()
      val rand: java.util.Random = new java.util.Random()

      rand.setSeed(srand.nextLong())

      val chars: Array[Char] = new Array[Char](length)
      chars.map(_ => validCharacters(rand.nextInt(validCharacters.length))).mkString("")
    }
  })

  register(
    "Convert vertex attribute to String", VertexAttributesOperations, new ProjectTransformation(_) {
      params +=
        Choice("attr", "Vertex attribute", options = project.vertexAttrList, multipleChoice = true)
      def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes.")
      def apply() = {
        for (attr <- splitParam("attr")) {
          project.vertexAttributes(attr) = project.vertexAttributes(attr).asString
        }
      }
    })

  register(
    "Convert edge attribute to String", EdgeAttributesOperations, new ProjectTransformation(_) {
      params +=
        Choice("attr", "Edge attribute", options = project.edgeAttrList, multipleChoice = true)
      def enabled = FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes.")
      def apply() = {
        for (attr <- splitParam("attr")) {
          project.edgeAttributes(attr) = project.edgeAttributes(attr).asString
        }
      }
    })

  register(
    "Convert vertex attribute to Double", VertexAttributesOperations, new ProjectTransformation(_) {
      def eligible =
        project.vertexAttrList[String] ++
          project.vertexAttrList[Long] ++
          project.vertexAttrList[Int]
      params += Choice("attr", "Vertex attribute", options = eligible, multipleChoice = true)
      def enabled = FEStatus.assert(eligible.nonEmpty, "No eligible vertex attributes.")
      def apply() = {
        for (name <- splitParam("attr")) {
          val attr = project.vertexAttributes(name)
          project.vertexAttributes(name) = toDouble(attr)
        }
      }
    })

  register(
    "Convert edge attribute to Double", EdgeAttributesOperations, new ProjectTransformation(_) {
      def eligible =
        project.edgeAttrList[String] ++
          project.edgeAttrList[Long] ++
          project.edgeAttrList[Int]
      params += Choice("attr", "Edge attribute", options = eligible, multipleChoice = true)
      def enabled = FEStatus.assert(eligible.nonEmpty, "No eligible edge attributes.")
      def apply() = {
        for (name <- splitParam("attr")) {
          val attr = project.edgeAttributes(name)
          project.edgeAttributes(name) = toDouble(attr)
        }
      }
    })

  register(
    "Convert vertex attributes to position",
    VertexAttributesOperations, new ProjectTransformation(_) {
      params ++= List(
        Param("output", "Save as", defaultValue = "position"),
        Choice("x", "X or latitude", options = project.vertexAttrList[Double]),
        Choice("y", "Y or longitude", options = project.vertexAttrList[Double]))
      def enabled = FEStatus.assert(
        project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
      def apply() = {
        assert(params("output").nonEmpty, "Please set an attribute name.")
        val paramX = params("x")
        val paramY = params("y")
        val pos = {
          val op = graph_operations.JoinAttributes[Double, Double]()
          val x = project.vertexAttributes(paramX).runtimeSafeCast[Double]
          val y = project.vertexAttributes(paramY).runtimeSafeCast[Double]
          op(op.a, x)(op.b, y).result.attr
        }
        project.newVertexAttribute(params("output"), pos, s"($paramX, $paramY)" + help)
      }
    })

  register("Replace with edge graph", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.EdgeGraph()
      val g = op(op.es, project.edgeBundle).result
      project.setVertexSet(g.newVS, idAttr = "id")
      project.edgeBundle = g.newES
    }
  })

  register("Derive vertex attribute", VertexAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("type", "Result type", options = FEOption.jsDataTypes),
      Choice("defined_attrs", "Only run on defined attributes",
        options = FEOption.bools), // Default is true.
      Code("expr", "Value", defaultValue = "", language = "javascript"))
    def enabled = project.hasVertexSet
    override def summary = {
      val name = params("output")
      val expr = params("expr")
      s"Derive vertex attribute: $name = $expr"
    }
    def apply() = {
      assert(params("output").nonEmpty, "Please set an output attribute name.")
      val expr = params("expr")
      val vertexSet = project.vertexSet
      val namedAttributes = JSUtilities.collectIdentifiers[Attribute[_]](project.vertexAttributes, expr)
      val namedScalars = JSUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val onlyOnDefinedAttrs = params("defined_attrs").toBoolean

      val result = params("type") match {
        case "String" =>
          graph_operations.DeriveJS.deriveFromAttributes[String](
            expr, namedAttributes, vertexSet, namedScalars, onlyOnDefinedAttrs)
        case "Double" =>
          graph_operations.DeriveJS.deriveFromAttributes[Double](
            expr, namedAttributes, vertexSet, namedScalars, onlyOnDefinedAttrs)
        case "Vector of Strings" =>
          graph_operations.DeriveJS.deriveFromAttributes[Vector[String]](
            expr, namedAttributes, vertexSet, namedScalars, onlyOnDefinedAttrs)
        case "Vector of Doubles" =>
          graph_operations.DeriveJS.deriveFromAttributes[Vector[Double]](
            expr, namedAttributes, vertexSet, namedScalars, onlyOnDefinedAttrs)
      }
      project.newVertexAttribute(params("output"), result, expr + help)
    }
  })

  register("Derive edge attribute", EdgeAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("type", "Result type", options = FEOption.jsDataTypes),
      Choice("defined_attrs", "Only run on defined attributes",
        options = FEOption.bools), // Default is true.
      Code("expr", "Value", defaultValue = "", language = "javascript"))
    def enabled = project.hasEdgeBundle
    override def summary = {
      val name = params("output")
      val expr = params("expr")
      s"Derive edge attribute: $name = $expr"
    }
    def apply() = {
      val expr = params("expr")
      val edgeBundle = project.edgeBundle
      val idSet = project.edgeBundle.idSet
      val namedEdgeAttributes = JSUtilities.collectIdentifiers[Attribute[_]](project.edgeAttributes, expr)
      val namedSrcVertexAttributes =
        JSUtilities.collectIdentifiers[Attribute[_]](project.vertexAttributes, expr, "src$")
          .map {
            case (name, attr) =>
              "src$" + name -> graph_operations.VertexToEdgeAttribute.srcAttribute(attr, edgeBundle)
          }
      val namedScalars = JSUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val namedDstVertexAttributes =
        JSUtilities.collectIdentifiers[Attribute[_]](project.vertexAttributes, expr, "dst$")
          .map {
            case (name, attr) =>
              "dst$" + name -> graph_operations.VertexToEdgeAttribute.dstAttribute(attr, edgeBundle)
          }

      val namedAttributes =
        namedEdgeAttributes ++ namedSrcVertexAttributes ++ namedDstVertexAttributes
      val onlyOnDefinedAttrs = params("defined_attrs").toBoolean

      val result = params("type") match {
        case "String" =>
          graph_operations.DeriveJS.deriveFromAttributes[String](
            expr, namedAttributes, idSet, namedScalars, onlyOnDefinedAttrs)
        case "Double" =>
          graph_operations.DeriveJS.deriveFromAttributes[Double](
            expr, namedAttributes, idSet, namedScalars, onlyOnDefinedAttrs)
        case "Vector of Strings" =>
          graph_operations.DeriveJS.deriveFromAttributes[Vector[String]](
            expr, namedAttributes, idSet, namedScalars, onlyOnDefinedAttrs)
        case "Vector of Doubles" =>
          graph_operations.DeriveJS.deriveFromAttributes[Vector[Double]](
            expr, namedAttributes, idSet, namedScalars, onlyOnDefinedAttrs)
      }
      project.newEdgeAttribute(params("output"), result, expr + help)
    }
  })

  register("Derive scalar", GlobalOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("type", "Result type", options = FEOption.list("Double", "String")),
      Code("expr", "Value", defaultValue = "", language = "javascript"))
    def enabled = FEStatus.enabled
    override def summary = {
      val name = params("output")
      val expr = params("expr")
      s"Derive scalar: $name = $expr"
    }
    def apply() = {
      val expr = params("expr")
      val namedScalars = JSUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val result = params("type") match {
        case "String" =>
          graph_operations.DeriveJSScalar.deriveFromScalars[String](expr, namedScalars)
        case "Double" =>
          graph_operations.DeriveJSScalar.deriveFromScalars[Double](expr, namedScalars)
      }
      project.newScalar(params("output"), result.sc, expr + help)
    }
  })

  register("Predict vertex attribute", MachineLearningOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
      Choice("features", "Predictors", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice("method", "Method", options = FEOption.list(
        "Linear regression", "Ridge regression", "Lasso", "Logistic regression", "Naive Bayes",
        "Decision tree", "Random forest", "Gradient-boosted trees")))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    override def summary = {
      val method = params("method").capitalize
      val label = params("label")
      s"$method for $label"
    }
    def apply() = {
      assert(params("features").nonEmpty, "Please select at least one predictor.")
      val featureNames = splitParam("features")
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val method = params("method")
      val prediction = {
        val op = graph_operations.Regression(method, features.size)
        op(op.label, label)(op.features, features).result.prediction
      }
      project.newVertexAttribute(s"${labelName}_prediction", prediction, s"$method for $labelName")
    }
  })

  register("Train linear regression model", MachineLearningOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice("method", "Method", options = FEOption.list(
        "Linear regression", "Ridge regression", "Lasso")))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    override def summary = {
      val method = params("method").capitalize
      val label = params("label")
      s"Build a $method model for $label"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one predictor.")
      val featureNames = splitParam("features").sorted
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val name = params("name")
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val method = params("method")
      val model = {
        val op = graph_operations.RegressionModelTrainer(
          method, labelName, featureNames.toList)
        op(op.label, label)(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Train a logistic regression model", MachineLearningOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("max_iter", "Maximum number of iterations", default = 20))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one feature.")
      val featureNames = splitParam("features").sorted
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val name = params("name")
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val maxIter = params("max_iter").toInt
      val model = {
        val op = graph_operations.LogisticRegressionModelTrainer(
          maxIter, labelName, featureNames.toList)
        op(op.label, label)(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Train a decision tree classification model", MachineLearningOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice("impurity", "Impurity", options = FEOption.list("entropy", "gini")),
      NonNegInt("maxBins", "Maximum number of bins", default = 32),
      NonNegInt("maxDepth", "Maximum depth of tree", default = 5),
      NonNegDouble("minInfoGain", "Minimum information gain for splits", defaultValue = "0.0"),
      NonNegInt("minInstancesPerNode", "Minimum size of children after splits", default = 1),
      RandomSeed("seed", "Seed"))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one feature.")
      val labelName = params("label")
      val featureNames = params("features").split(",", -1).sorted
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val model = {
        val op = graph_operations.TrainDecisionTreeClassifier(
          labelName = labelName,
          featureNames = featureNames.toList,
          impurity = params("impurity"),
          maxBins = params("maxBins").toInt,
          maxDepth = params("maxDepth").toInt,
          minInfoGain = params("minInfoGain").toDouble,
          minInstancesPerNode = params("minInstancesPerNode").toInt,
          seed = params("seed").toInt)
        op(op.label, label)(op.features, features).result.model
      }
      val name = params("name")
      project.scalars(name) = model
    }
  })

  register("Train a decision tree regression model", MachineLearningOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("maxBins", "Maximum number of bins", default = 32),
      NonNegInt("maxDepth", "Maximum depth of tree", default = 5),
      NonNegDouble("minInfoGain", "Minimum information gain for splits", defaultValue = "0.0"),
      NonNegInt("minInstancesPerNode", "Minimum size of children after splits", default = 1),
      RandomSeed("seed", "Seed"))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one feature.")
      val labelName = params("label")
      val featureNames = params("features").split(",", -1).sorted
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val model = {
        val op = graph_operations.TrainDecisionTreeRegressor(
          labelName = labelName,
          featureNames = featureNames.toList,
          impurity = "variance",
          maxBins = params("maxBins").toInt,
          maxDepth = params("maxDepth").toInt,
          minInfoGain = params("minInfoGain").toDouble,
          minInstancesPerNode = params("minInstancesPerNode").toInt,
          seed = params("seed").toInt)
        op(op.label, label)(op.features, features).result.model
      }
      val name = params("name")
      project.scalars(name) = model
    }
  })

  register("Train a k-means clustering model", MachineLearningOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice(
        "features", "Attributes",
        options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("k", "Number of clusters", default = 2),
      NonNegInt("max_iter", "Maximum number of iterations", default = 20),
      RandomSeed("seed", "Seed"))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    override def summary = {
      val k = params("k")
      s"Train a k-means clustering model (k=$k)"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one predictor.")
      val featureNames = splitParam("features").sorted
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val name = params("name")
      val k = params("k").toInt
      val maxIter = params("max_iter").toInt
      val seed = params("seed").toLong
      val model = {
        val op = graph_operations.KMeansClusteringModelTrainer(
          k, maxIter, seed, featureNames.toList)
        op(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Predict from model", MachineLearningOperations, new ProjectTransformation(_) {
    def models = project.viewer.models.filterNot(_._2.isClassification)
    params ++= List(
      Param("name", "The name of the attribute of the predictions"),
      ModelParams("model", "The parameters of the model", models, project.vertexAttrList[Double]))
    def enabled =
      FEStatus.assert(models.nonEmpty, "No regression models.") &&
        FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of attribute.")
      assert(params("model").nonEmpty, "Please select a model.")
      val name = params("name")
      val p = json.Json.parse(params("model"))
      val modelName = (p \ "modelName").as[String]
      val modelValue: Scalar[model.Model] = project.scalars(modelName).runtimeSafeCast[model.Model]
      val features = (p \ "features").as[List[String]].map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val predictedAttribute = {
        val op = graph_operations.PredictFromModel(features.size)
        op(op.model, modelValue)(op.features, features).result.prediction
      }
      project.newVertexAttribute(name, predictedAttribute, s"predicted from ${modelName}")
    }
  })

  register("Classify vertices with a model", MachineLearningOperations, new ProjectTransformation(_) {
    def models = project.viewer.models.filter(_._2.isClassification)
    params ++= List(
      Param("name", "The name of the attribute of the classifications"),
      ModelParams("model", "The parameters of the model", models, project.vertexAttrList[Double]))
    def enabled =
      FEStatus.assert(models.nonEmpty, "No classification models.") &&
        FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of attribute.")
      assert(params("model").nonEmpty, "Please select a model.")
      val name = params("name")
      val p = json.Json.parse(params("model"))
      val modelName = (p \ "modelName").as[String]
      val modelValue: Scalar[model.Model] = project.scalars(modelName).runtimeSafeCast[model.Model]
      val features = (p \ "features").as[List[String]].map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      import model.Implicits._
      val generatesProbability = modelValue.modelMeta.generatesProbability
      val isBinary = modelValue.modelMeta.isBinary
      val op = graph_operations.ClassifyWithModel(features.size)
      val result = op(op.model, modelValue)(op.features, features).result
      val classifiedAttribute = result.classification
      project.newVertexAttribute(name, classifiedAttribute,
        s"classification according to ${modelName}")
      if (generatesProbability) {
        val certainty = result.probability
        project.newVertexAttribute(name + "_certainty", certainty,
          s"probability of predicted class according to ${modelName}")
        if (isBinary) {
          val probabilityOf0 = graph_operations.DeriveJS.deriveFromAttributes[Double](
            "classification == 0 ? certainty : 1 - certainty",
            Seq("certainty" -> certainty, "classification" -> classifiedAttribute),
            project.vertexSet)
          project.newVertexAttribute(name + "_probability_of_0", probabilityOf0,
            s"probability of class 0 according to ${modelName}")
          val probabilityOf1 = graph_operations.DeriveJS.deriveFromAttributes[Double](
            "1 - probabilityOf0", Seq("probabilityOf0" -> probabilityOf0), project.vertexSet)
          project.newVertexAttribute(name + "_probability_of_1", probabilityOf1,
            s"probability of class 1 according to ${modelName}")
        }
      }
    }
  })

  register("Aggregate to segmentation", PropagationOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = params ++= aggregateParams(parent.vertexAttributes)
    def enabled =
      project.assertSegmentation &&
        FEStatus.assert(parent.vertexAttributes.nonEmpty,
          "No vertex attributes on parent")
    def apply() = {
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          seg.belongsTo,
          AttributeWithLocalAggregator(parent.vertexAttributes(attr), choice))
        project.newVertexAttribute(s"${attr}_${choice}", result)
      }
    }
  })

  register(
    "Weighted aggregate to segmentation", PropagationOperations, new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters = {
        params += Choice("weight", "Weight", options = project.parentVertexAttrList[Double])
        params ++= aggregateParams(parent.vertexAttributes, weighted = true)
      }
      def enabled =
        project.assertSegmentation &&
          FEStatus.assert(parent.vertexAttributeNames[Double].nonEmpty,
            "No numeric vertex attributes on parent")
      def apply() = {
        val weightName = params("weight")
        val weight = parent.vertexAttributes(weightName).runtimeSafeCast[Double]
        for ((attr, choice) <- parseAggregateParams(params)) {
          val result = aggregateViaConnection(
            seg.belongsTo,
            AttributeWithWeightedAggregator(weight, parent.vertexAttributes(attr), choice))
          project.newVertexAttribute(s"${attr}_${choice}_by_${weightName}", result)
        }
      }
    })

  register("Aggregate from segmentation", PropagationOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {
      params += Param(
        "prefix", "Generated name prefix", defaultValue = project.asSegmentation.segmentationName)
      params ++= aggregateParams(project.vertexAttributes)
    }
    def enabled =
      project.assertSegmentation &&
        FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          seg.belongsTo.reverse,
          AttributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        seg.parent.newVertexAttribute(s"${prefix}${attr}_${choice}", result)
      }
    }
  })

  register(
    "Weighted aggregate from segmentation", PropagationOperations, new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters = params ++= List(
        Param("prefix", "Generated name prefix",
          defaultValue = project.asSegmentation.segmentationName),
        Choice("weight", "Weight", options = project.vertexAttrList[Double])) ++
        aggregateParams(project.vertexAttributes, weighted = true)
      def enabled =
        project.assertSegmentation &&
          FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes")
      def apply() = {
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        val weightName = params("weight")
        val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
        for ((attr, choice) <- parseAggregateParams(params)) {
          val result = aggregateViaConnection(
            seg.belongsTo.reverse,
            AttributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
          seg.parent.newVertexAttribute(s"${prefix}${attr}_${choice}_by_${weightName}", result)
        }
      }
    })

  register("Create edges from set overlaps", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters =
      params += NonNegInt("minOverlap", "Minimal overlap for connecting two segments", default = 3)
    def enabled = project.assertSegmentation
    def apply() = {
      val op = graph_operations.SetOverlap(params("minOverlap").toInt)
      val res = op(op.belongsTo, seg.belongsTo).result
      project.edgeBundle = res.overlaps
      // Long is better supported on the frontend than Int.
      project.edgeAttributes("Overlap size") = res.overlapSize.asLong
    }
  })

  private def segmentationSizesSquareSum(seg: SegmentationEditor, parent: ProjectEditor)(
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

  register("Create edges from co-occurrence", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    override def visibleScalars =
      if (project.isSegmentation) {
        val scalar = segmentationSizesSquareSum(seg, parent)
        implicit val entityProgressManager = env.entityProgressManager
        List(ProjectViewer.feScalar(scalar, "num_created_edges", "", Map()))
      } else {
        List()
      }

    def enabled =
      project.assertSegmentation &&
        FEStatus.assert(parent.edgeBundle == null, "Parent graph has edges already.")
    def apply() = {
      val op = graph_operations.EdgesFromSegmentation()
      val result = op(op.belongsTo, seg.belongsTo).result
      parent.edgeBundle = result.es
      for ((name, attr) <- project.vertexAttributes) {
        parent.edgeAttributes(s"${seg.segmentationName}_$name") = attr.pullVia(result.origin)
      }
    }
  })

  register("Sample edges from co-occurrence", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = params ++= List(
      NonNegDouble("probability", "Vertex pair selection probability", defaultValue = "0.001"),
      RandomSeed("seed", "Random seed"))
    override def visibleScalars =
      if (project.isSegmentation) {
        val scalar = segmentationSizesSquareSum(seg, parent)
        implicit val entityProgressManager = env.entityProgressManager
        List(ProjectViewer.feScalar(scalar, "num_total_edges", "", Map()))
      } else {
        List()
      }

    def enabled =
      project.assertSegmentation &&
        FEStatus.assert(parent.edgeBundle == null, "Parent graph has edges already.")
    def apply() = {
      val op = graph_operations.SampleEdgesFromSegmentation(
        params("probability").toDouble,
        params("seed").toLong)
      val result = op(op.belongsTo, seg.belongsTo).result
      parent.edgeBundle = result.es
      parent.edgeAttributes("multiplicity") = result.multiplicity
    }
  })

  register("Pull segmentation one level up", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}

    def enabled =
      project.assertSegmentation && FEStatus.assert(parent.isSegmentation, "Parent graph is not a segmentation")

    def apply() = {
      val parentSegmentation = parent.asSegmentation
      val thisSegmentation = project.asSegmentation
      val segmentationName = thisSegmentation.segmentationName
      val targetSegmentation = parentSegmentation.parent.segmentation(segmentationName)
      targetSegmentation.state = thisSegmentation.state
      targetSegmentation.belongsTo =
        parentSegmentation.belongsTo.concat(thisSegmentation.belongsTo)
    }
  })

  register("Grow segmentation", StructureOperations, new ProjectTransformation(_) with SegOp {
    def enabled = project.assertSegmentation && project.hasVertexSet &&
      FEStatus.assert(parent.edgeBundle != null, "Parent has no edges.")

    def addSegmentationParameters =
      params += Choice("direction", "Direction", options = Direction.neighborOptions)

    def apply() = {
      val segmentation = project.asSegmentation
      val direction = Direction(params("direction"), parent.edgeBundle, reversed = true)

      val op = graph_operations.GrowSegmentation()
      segmentation.belongsTo = op(
        op.vsG, parent.vertexSet)(
          op.esG, direction.edgeBundle)(
            op.esGS, segmentation.belongsTo).result.esGS
    }
  })

  register("Aggregate on neighbors", PropagationOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Choice("direction", "Aggregate on", options = Direction.options)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes") && project.hasEdgeBundle
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = Direction(params("direction"), project.edgeBundle).edgeBundle
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          edges,
          AttributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        project.newVertexAttribute(s"${prefix}${attr}_${choice}", result)
      }
    }
  })

  register("Weighted aggregate on neighbors", PropagationOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Choice("weight", "Weight", options = project.vertexAttrList[Double]),
      Choice("direction", "Aggregate on", options = Direction.options)) ++
      aggregateParams(project.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes") &&
        project.hasEdgeBundle
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = Direction(params("direction"), project.edgeBundle).edgeBundle
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((name, choice) <- parseAggregateParams(params)) {
        val attr = project.vertexAttributes(name)
        val result = aggregateViaConnection(
          edges,
          AttributeWithWeightedAggregator(weight, attr, choice))
        project.newVertexAttribute(s"${prefix}${name}_${choice}_by_${weightName}", result)
      }
    }
  })

  register("Split vertices", StructureOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("rep", "Repetition attribute", options = project.vertexAttrList[Double]),
      Param("idattr", "ID attribute name", defaultValue = "new_id"),
      Param("idx", "Index attribute name", defaultValue = "index"))

    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No Double vertex attributes")
    def doSplit(doubleAttr: Attribute[Double]): graph_operations.SplitVertices.Output = {
      val op = graph_operations.SplitVertices()
      op(op.attr, doubleAttr.asLong).result
    }
    def apply() = {
      val rep = params("rep")
      val split = doSplit(project.vertexAttributes(rep).runtimeSafeCast[Double])

      project.pullBack(split.belongsTo)
      project.vertexAttributes(params("idx")) = split.indexAttr
      project.newVertexAttribute(params("idattr"), project.vertexSet.idAttribute)
    }
  })

  register("Split edges", StructureOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("rep", "Repetition attribute", options = project.edgeAttrList[Double]),
      Param("idx", "Index attribute name", defaultValue = "index"))

    def enabled =
      FEStatus.assert(project.edgeAttrList[Double].nonEmpty, "No Double edge attributes")
    def doSplit(doubleAttr: Attribute[Double]): graph_operations.SplitEdges.Output = {
      val op = graph_operations.SplitEdges()
      op(op.es, project.edgeBundle)(op.attr, doubleAttr.asLong).result
    }
    def apply() = {
      val rep = params("rep")
      val split = doSplit(project.edgeAttributes(rep).runtimeSafeCast[Double])

      project.pullBackEdges(
        project.edgeBundle, project.edgeAttributes.toIndexedSeq, split.newEdges, split.belongsTo)
      project.edgeAttributes(params("idx")) = split.indexAttr
    }
  })

  register("Merge vertices by attribute", StructureOperations, new ProjectTransformation(_) {
    params += Choice("key", "Match by", options = project.vertexAttrList)
    params ++= aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def merge[T](attr: Attribute[T]): graph_operations.MergeVertices.Output = {
      val op = graph_operations.MergeVertices[T]()
      op(op.attr, attr).result
    }
    def apply() = {
      val key = params("key")
      val m = merge(project.vertexAttributes(key))
      val oldVAttrs = project.vertexAttributes.toMap
      val oldEdges = project.edgeBundle
      val oldEAttrs = project.edgeAttributes.toMap
      val oldSegmentations = project.viewer.segmentationMap
      val oldBelongsTo = if (project.isSegmentation) project.asSegmentation.belongsTo else null
      project.setVertexSet(m.segments, idAttr = "id")
      for ((name, segViewer) <- oldSegmentations) {
        val seg = project.segmentation(name)
        seg.segmentationState = segViewer.segmentationState
        val op = graph_operations.InducedEdgeBundle(induceDst = false)
        seg.belongsTo = op(
          op.srcMapping, m.belongsTo)(
            op.edges, seg.belongsTo).result.induced
      }
      if (project.isSegmentation) {
        val seg = project.asSegmentation
        val op = graph_operations.InducedEdgeBundle(induceSrc = false)
        seg.belongsTo = op(
          op.dstMapping, m.belongsTo)(
            op.edges, oldBelongsTo).result.induced
      }
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          m.belongsTo,
          AttributeWithLocalAggregator(oldVAttrs(attr), choice))
        project.newVertexAttribute(s"${attr}_${choice}", result)
      }
      // Automatically keep the key attribute.
      project.vertexAttributes(key) = aggregateViaConnection(
        m.belongsTo,
        AttributeWithAggregator(oldVAttrs(key), "first"))
      if (oldEdges != null) {
        val edgeInduction = {
          val op = graph_operations.InducedEdgeBundle()
          op(op.srcMapping, m.belongsTo)(op.dstMapping, m.belongsTo)(op.edges, oldEdges).result
        }
        project.edgeBundle = edgeInduction.induced
        for ((name, eAttr) <- oldEAttrs) {
          project.edgeAttributes(name) = eAttr.pullVia(edgeInduction.embedding)
        }
      }
    }
  })

  private def mergeEdgesWithKey[T](edgesAsAttr: Attribute[(ID, ID)], keyAttr: Attribute[T]) = {
    val edgesAndKey: Attribute[((ID, ID), T)] = edgesAsAttr.join(keyAttr)
    val op = graph_operations.MergeVertices[((ID, ID), T)]()
    op(op.attr, edgesAndKey).result
  }

  private def mergeEdges(edgesAsAttr: Attribute[(ID, ID)]) = {
    val op = graph_operations.MergeVertices[(ID, ID)]()
    op(op.attr, edgesAsAttr).result
  }

  // Common code for operations "merge parallel edges" and "merge parallel edges by key"
  private def applyMergeParallelEdges(
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

  register("Merge parallel edges", StructureOperations, new ProjectTransformation(_) {
    params ++= aggregateParams(project.edgeAttributes)
    def enabled = project.hasEdgeBundle

    def apply() = {
      applyMergeParallelEdges(project, params, byKey = false)
    }
  })

  register("Merge parallel edges by attribute", StructureOperations, new ProjectTransformation(_) {
    params += Choice("key", "Merge by", options = project.edgeAttrList)
    params ++= aggregateParams(project.edgeAttributes)
    def enabled = FEStatus.assert(project.edgeAttrList.nonEmpty,
      "There must be at least one edge attribute")

    def apply() = {
      applyMergeParallelEdges(project, params, byKey = true)
    }
  })

  register("Merge parallel segmentation links", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    def enabled = project.assertSegmentation
    def apply() = {
      val linksAsAttr = {
        val op = graph_operations.EdgeBundleAsAttribute()
        op(op.edges, seg.belongsTo).result.attr
      }
      val mergedResult = mergeEdges(linksAsAttr)
      val newLinks = {
        val op = graph_operations.PulledOverEdges()
        op(op.originalEB, seg.belongsTo)(op.injection, mergedResult.representative)
          .result.pulledEB
      }
      seg.belongsTo = newLinks
    }
  })

  register("Discard loop edges", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      val edgesAsAttr = {
        val op = graph_operations.EdgeBundleAsAttribute()
        op(op.edges, project.edgeBundle).result.attr
      }
      val guid = edgesAsAttr.entity.gUID.toString
      val embedding = FEFilters.embedFilteredVertices(
        project.edgeBundle.idSet,
        Seq(FEVertexAttributeFilter(guid, "!=")))
      project.pullBackEdges(embedding)
    }
  })

  register("Replace edges with triadic closure", StructureOperations, new ProjectTransformation(_) {
    def enabled = project.hasVertexSet && project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.ConcatenateBundlesMulti()
      val result = op(op.edgesAB, project.edgeBundle)(
        op.edgesBC, project.edgeBundle).result

      // saving attributes and original edges
      val origEdgeAttrs = project.edgeAttributes.toIndexedSeq

      // new edges after closure
      project.edgeBundle = result.edgesAC

      // pulling old edge attributes
      for ((name, attr) <- origEdgeAttrs) {
        project.newEdgeAttribute("ab_" + name, attr.pullVia(result.projectionFirst))
        project.newEdgeAttribute("bc_" + name, attr.pullVia(result.projectionSecond))
      }
    }
  })

  register("Create snowball sample", StructureOperations, new ProjectTransformation(_) {
    params ++= List(
      Ratio("ratio", "Fraction of vertices to use as starting points", defaultValue = "0.0001"),
      NonNegInt("radius", "Radius", default = 3),
      Param("attrName", "Attribute name", defaultValue = "distance_from_start_point"),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet && project.hasEdgeBundle
    def apply() = {
      val ratio = params("ratio")
      // Creating random attr for filtering the original center vertices of the "snowballs".
      val rnd = {
        val op = graph_operations.AddRandomAttribute(params("seed").toInt, "Standard Uniform")
        op(op.vs, project.vertexSet).result.attr
      }

      // Creating derived attribute based on rnd and ratio parameter.
      val startingDistance = rnd.deriveX[Double](s"x < ${ratio} ? 0.0 : undefined")

      // Constant unit length for all edges.
      val edgeLength = project.edgeBundle.const(1.0)

      // Running shortest path from vertices with attribute startingDistance.
      val distance = {
        val op = graph_operations.ShortestPath(params("radius").toInt)
        op(op.vs, project.vertexSet)(
          op.es, project.edgeBundle)(
            op.edgeDistance, edgeLength)(
              op.startingDistance, startingDistance).result.distance
      }
      project.newVertexAttribute(params("attrName"), distance)

      // Filtering on distance attribute.
      val guid = distance.entity.gUID.toString
      val vertexEmbedding = FEFilters.embedFilteredVertices(
        project.vertexSet, Seq(FEVertexAttributeFilter(guid, ">-1")), heavy = true)
      project.pullBack(vertexEmbedding)

    }
  })

  register("Sample graph by random walks", StructureOperations, new ProjectTransformation(_) {
    params ++= List(
      NonNegInt("startPoints", "Number of start points", default = 1),
      NonNegInt("walksFromOnePoint", "Number of walks from each start point", default = 10000),
      Ratio("walkAbortionProbability", "Walk abortion probability", defaultValue = "0.15"),
      Param("vertexAttrName", "Save vertex indices as", defaultValue = "first_reached"),
      Param("edgeAttrName", "Save edge indices as", defaultValue = "firts_traversed"),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet && project.hasEdgeBundle

    def apply() = {
      val output = {
        val startPoints = params("startPoints").toInt
        val walksFromOnePoint = params("walksFromOnePoint").toInt
        val walkAbortionProbability = params("walkAbortionProbability").toDouble
        val seed = params("seed").toInt
        val op = graph_operations.RandomWalkSample(startPoints, walksFromOnePoint,
          walkAbortionProbability, seed)
        op(op.vs, project.vertexSet)(op.es, project.edgeBundle).result
      }
      project.newVertexAttribute(params("vertexAttrName"), output.vertexFirstVisited)
      project.newEdgeAttribute(params("edgeAttrName"), output.edgeFirstTraversed)
    }
  })

  register("Aggregate vertex attribute globally", GlobalOperations, new ProjectTransformation(_) {
    params += Param("prefix", "Generated name prefix")
    params ++= aggregateParams(project.vertexAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(AttributeWithAggregator(project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register("Weighted aggregate vertex attribute globally", GlobalOperations,
    new ProjectTransformation(_) {
      params ++= List(
        Param("prefix", "Generated name prefix"),
        Choice("weight", "Weight", options = project.vertexAttrList[Double])) ++
        aggregateParams(project.vertexAttributes, needsGlobal = true, weighted = true)
      def enabled =
        FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes")
      def apply() = {
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        val weightName = params("weight")
        val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
        for ((attr, choice) <- parseAggregateParams(params)) {
          val result = aggregate(
            AttributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
          val name = s"${prefix}${attr}_${choice}_by_${weightName}"
          project.scalars(name) = result
        }
      }
    })

  register("Aggregate edge attribute globally", GlobalOperations, new ProjectTransformation(_) {
    params += Param("prefix", "Generated name prefix")
    params ++= aggregateParams(project.edgeAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithAggregator(project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register("Weighted aggregate edge attribute globally", GlobalOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("prefix", "Generated name prefix"),
      Choice("weight", "Weight", options = project.edgeAttrList[Double])) ++
      aggregateParams(
        project.edgeAttributes,
        needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(project.edgeAttrList[Double].nonEmpty, "No numeric edge attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithWeightedAggregator(weight, project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}_by_${weightName}"
        project.scalars(name) = result
      }
    }
  })

  register("Aggregate edge attribute to vertices", PropagationOperations, new ProjectTransformation(_) {
    params ++= List(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Choice("direction", "Aggregate on", options = Direction.attrOptions)) ++
      aggregateParams(project.edgeAttributes)
    def enabled =
      FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    def apply() = {
      val direction = Direction(params("direction"), project.edgeBundle)
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          direction.edgeBundle,
          AttributeWithLocalAggregator(
            direction.pull(project.edgeAttributes(attr)),
            choice))
        project.newVertexAttribute(s"${prefix}${attr}_${choice}", result)
      }
    }
  })

  register(
    "Weighted aggregate edge attribute to vertices", PropagationOperations,
    new ProjectTransformation(_) {
      params ++= List(
        Param("prefix", "Generated name prefix", defaultValue = "edge"),
        Choice("weight", "Weight", options = project.edgeAttrList[Double]),
        Choice("direction", "Aggregate on", options = Direction.attrOptions)) ++
        aggregateParams(
          project.edgeAttributes,
          weighted = true)
      def enabled =
        FEStatus.assert(project.edgeAttrList[Double].nonEmpty, "No numeric edge attributes")
      def apply() = {
        val direction = Direction(params("direction"), project.edgeBundle)
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        val weightName = params("weight")
        val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
        for ((attr, choice) <- parseAggregateParams(params)) {
          val result = aggregateFromEdges(
            direction.edgeBundle,
            AttributeWithWeightedAggregator(
              direction.pull(weight),
              direction.pull(project.edgeAttributes(attr)),
              choice))
          project.newVertexAttribute(s"${prefix}${attr}_${choice}_by_${weightName}", result)
        }
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

  register("Copy graph into a segmentation", CreateSegmentationOperations, new ProjectTransformation(_) {
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

  register("Import project as segmentation", StructureOperations, "project", "segmentation")(
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

  registerOp(
    "Import segmentation links", defaultIcon, ImportOperations,
    inputs = List(projectInput, "links"), outputs = List(projectOutput),
    factory = new ProjectOutputOperation(_) {
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

  registerOp(
    "Import segmentation", defaultIcon, ImportOperations,
    inputs = List(projectInput, "segmentation"),
    outputs = List(projectOutput),
    factory = new ProjectOutputOperation(_) {
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

  register("Define segmentation links from matching attributes",
    StructureOperations, new ProjectTransformation(_) with SegOp {
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

  register("Copy scalar from other project", StructureOperations, "project", "scalar")(
    new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val them = projectInput("scalar")
      params ++= List(
        Choice("scalar", "Name of the scalar to copy", options = them.scalarList),
        Param("save_as", "Save as"))
      def enabled = FEStatus.assert(them.scalarNames.nonEmpty, "No scalars found.")
      def apply() = {
        val origName = params("scalar")
        val newName = params("save_as")
        val scalarName = if (newName.isEmpty) origName else newName
        project.scalars(scalarName) = them.scalars(origName)
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

  register("Fingerprint based on attributes", SpecialtyOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("leftName", "First ID attribute", options = project.vertexAttrList[String]),
      Choice("rightName", "Second ID attribute", options = project.vertexAttrList[String]),
      Choice("weights", "Edge weights",
        options = FEOption.noWeight +: project.edgeAttrList[Double]),
      NonNegInt("mo", "Minimum overlap", default = 1),
      Ratio("ms", "Minimum similarity", defaultValue = "0.5"),
      Param(
        "extra",
        "Fingerprinting algorithm additional parameters",
        defaultValue = ""))
    def enabled =
      project.hasEdgeBundle &&
        FEStatus.assert(project.vertexAttrList[String].size >= 2, "Two String attributes are needed.")
    def apply() = {
      val mo = params("mo").toInt
      val ms = params("ms").toDouble
      assert(mo >= 1, "Minimum overlap cannot be less than 1.")
      val leftName = project.vertexAttributes(params("leftName")).runtimeSafeCast[String]
      val rightName = project.vertexAttributes(params("rightName")).runtimeSafeCast[String]
      val weightsName = params("weights")
      val weights =
        if (weightsName == FEOption.noWeight.id) project.edgeBundle.const(1.0)
        else project.edgeAttributes(params("weights")).runtimeSafeCast[Double]

      val candidates = {
        val op = graph_operations.FingerprintingCandidates()
        op(op.es, project.edgeBundle)(op.leftName, leftName)(op.rightName, rightName)
          .result.candidates
      }
      val fingerprinting = {
        // TODO: This is a temporary hack to facilitate experimentation with the underlying backend
        // operation w/o too much disruption to users. Should be removed once we are clear on what
        // we want to provide for fingerprinting.
        val baseParams = s""""minimumOverlap": $mo, "minimumSimilarity": $ms"""
        val extraParams = params("extra")
        val paramsJson = if (extraParams == "") baseParams else (baseParams + ", " + extraParams)
        val op = graph_operations.Fingerprinting.fromJson(json.Json.parse(s"{$paramsJson}"))
        op(
          op.leftEdges, project.edgeBundle)(
            op.leftEdgeWeights, weights)(
              op.rightEdges, project.edgeBundle)(
                op.rightEdgeWeights, weights)(
                  op.candidates, candidates)
          .result
      }
      val newLeftName = leftName.pullVia(fingerprinting.matching.reverse)
      val newRightName = rightName.pullVia(fingerprinting.matching)

      project.scalars("fingerprinting matches found") = fingerprinting.matching.countScalar
      project.vertexAttributes(params("leftName")) = newLeftName.fallback(leftName)
      project.vertexAttributes(params("rightName")) = newRightName.fallback(rightName)
      project.newVertexAttribute(
        params("leftName") + " similarity score", fingerprinting.leftSimilarities)
      project.newVertexAttribute(
        params("rightName") + " similarity score", fingerprinting.rightSimilarities)
    }
  })

  register("Copy vertex attributes from segmentation", PropagationOperations,
    new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters =
        params += Param("prefix", "Attribute name prefix", defaultValue = seg.segmentationName)
      def enabled =
        project.assertSegmentation &&
          FEStatus.assert(project.vertexAttrList.size > 0, "No vertex attributes") &&
          FEStatus.assert(parent.vertexSet != null, s"No vertices on $parent") &&
          FEStatus.assert(seg.belongsTo.properties.isFunction,
            s"Vertices in base project are not guaranteed to be contained in only one segment")
      def apply() = {
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        for ((name, attr) <- project.vertexAttributes.toMap) {
          parent.newVertexAttribute(
            prefix + name,
            attr.pullVia(seg.belongsTo))
        }
      }
    })

  register("Copy vertex attributes to segmentation", PropagationOperations,
    new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters =
        params += Param("prefix", "Attribute name prefix")
      def enabled =
        project.assertSegmentation &&
          project.hasVertexSet &&
          FEStatus.assert(parent.vertexAttributes.size > 0,
            s"Parent $parent has no vertex attributes") &&
            FEStatus.assert(seg.belongsTo.properties.isReversedFunction,
              "Segments are not guaranteed to contain only one vertex")
      def apply() = {
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        for ((name, attr) <- parent.vertexAttributes.toMap) {
          project.newVertexAttribute(
            prefix + name,
            attr.pullVia(seg.belongsTo.reverse))
        }
      }
    })

  register("Compare segmentation edges", GlobalOperations, new ProjectTransformation(_) {
    def isCompatibleSegmentation(segmentation: SegmentationEditor): Boolean = {
      return segmentation.edgeBundle != null &&
        segmentation.belongsTo.properties.compliesWith(EdgeBundleProperties.identity)
    }

    val possibleSegmentations = FEOption.list(
      project.segmentations
        .filter(isCompatibleSegmentation)
        .map { seg => seg.segmentationName }
        .toList)

    params ++= List(
      Choice("golden", "Golden segmentation", options = possibleSegmentations),
      Choice("test", "Test segmentation", options = possibleSegmentations))
    def enabled = FEStatus.assert(
      possibleSegmentations.size >= 2,
      "At least two segmentations are needed. Both should have edges " +
        "and both have to contain the same vertices as the base project. " +
        "(For example, use the copy graph into segmentation operation.)")

    def apply() = {
      val golden = project.existingSegmentation(params("golden"))
      val test = project.existingSegmentation(params("test"))
      val op = graph_operations.CompareSegmentationEdges()
      val result = op(
        op.goldenBelongsTo, golden.belongsTo)(
          op.testBelongsTo, test.belongsTo)(
            op.goldenEdges, golden.edgeBundle)(
              op.testEdges, test.edgeBundle).result
      test.scalars("precision") = result.precision
      test.scalars("recall") = result.recall
      test.edgeAttributes("present_in_" + golden.segmentationName) = result.presentInGolden
      golden.edgeAttributes("present_in_" + test.segmentationName) = result.presentInTest
    }

  })

  register(
    "Link project and segmentation by fingerprint",
    SpecialtyOperations, new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters = params ++= List(
        NonNegInt("mo", "Minimum overlap", default = 1),
        Ratio("ms", "Minimum similarity", defaultValue = "0.0"),
        Param(
          "extra",
          "Fingerprinting algorithm additional parameters",
          defaultValue = ""))
      def enabled =
        project.assertSegmentation &&
          project.hasEdgeBundle && FEStatus.assert(parent.edgeBundle != null, s"No edges on $parent")
      def apply() = {
        val mo = params("mo").toInt
        val ms = params("ms").toDouble

        // We are setting the stage here for the generic fingerprinting operation. For a vertex A
        // on the left (base project) side and a vertex B on the right (segmentation) side we
        // want to "create" a common neighbor for fingerprinting purposes iff a neighbor of A (A') is
        // connected to a neigbor of B (B'). In practice, to make the setup symmetric, we will
        // actually create two common neighbors, namely we will connect both A and B to A' and B'.
        //
        // There is one more twist, that we want to consider A being connected to B directly also
        // as an evidence for A and B being a good match. To achieve this, we basically artificially
        // make every vertex a member of its own neighborhood by adding loop edges.
        val leftWithLoops = parallelEdgeBundleUnion(parent.edgeBundle, parent.vertexSet.loops)
        val rightWithLoops = parallelEdgeBundleUnion(project.edgeBundle, project.vertexSet.loops)
        val fromLeftToRight = leftWithLoops.concat(seg.belongsTo)
        val fromRightToLeft = rightWithLoops.concat(seg.belongsTo.reverse)
        val leftEdges = generalEdgeBundleUnion(leftWithLoops, fromLeftToRight)
        val rightEdges = generalEdgeBundleUnion(rightWithLoops, fromRightToLeft)

        val candidates = {
          val op = graph_operations.FingerprintingCandidatesFromCommonNeighbors()
          op(op.leftEdges, leftEdges)(op.rightEdges, rightEdges).result.candidates
        }

        val fingerprinting = {
          // TODO: This is a temporary hack to facilitate experimentation with the underlying backend
          // operation w/o too much disruption to users. Should be removed once we are clear on what
          // we want to provide for fingerprinting.
          val baseParams = s""""minimumOverlap": $mo, "minimumSimilarity": $ms"""
          val extraParams = params("extra")
          val paramsJson = if (extraParams == "") baseParams else (baseParams + ", " + extraParams)
          val op = graph_operations.Fingerprinting.fromJson(json.Json.parse(s"{$paramsJson}"))
          op(
            op.leftEdges, leftEdges)(
              op.leftEdgeWeights, leftEdges.const(1.0))(
                op.rightEdges, rightEdges)(
                  op.rightEdgeWeights, rightEdges.const(1.0))(
                    op.candidates, candidates)
            .result
        }

        project.scalars("fingerprinting matches found") = fingerprinting.matching.countScalar
        seg.belongsTo = fingerprinting.matching
        parent.newVertexAttribute(
          "fingerprinting_similarity_score", fingerprinting.leftSimilarities)
        project.newVertexAttribute(
          "fingerprinting_similarity_score", fingerprinting.rightSimilarities)
      }
    })

  register("Change project notes", UtilityOperations, new ProjectTransformation(_) {
    params += Param("notes", "New contents")
    def enabled = FEStatus.enabled
    def apply() = {
      project.notes = params("notes")
    }
  })

  register(
    "Predict attribute by viral modeling",
    SpecialtyOperations, new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters = params ++= List(
        Param("prefix", "Generated name prefix", defaultValue = "viral"),
        Choice("target", "Target attribute",
          options = FEOption.list(parentDoubleAttributes)),
        Ratio("test_set_ratio", "Test set ratio", defaultValue = "0.1"),
        RandomSeed("seed", "Random seed for test set selection"),
        NonNegDouble("max_deviation", "Maximal segment deviation", defaultValue = "1.0"),
        NonNegInt("min_num_defined", "Minimum number of defined attributes in a segment", default = 3),
        Ratio("min_ratio_defined", "Minimal ratio of defined attributes in a segment", defaultValue = "0.25"),
        NonNegInt("iterations", "Iterations", default = 3))
      def parentDoubleAttributes = parent.vertexAttributeNames[Double].toList
      def enabled =
        project.assertSegmentation &&
          project.hasVertexSet &&
          FEStatus.assert(FEOption.list(parentDoubleAttributes).nonEmpty,
            "No numeric vertex attributes.")
      def apply() = {
        // partition target attribute to test and train sets
        val targetName = params("target")
        val target = parent.vertexAttributes(targetName).runtimeSafeCast[Double]
        val roles = {
          val op = graph_operations.CreateRole(params("test_set_ratio").toDouble, params("seed").toInt)
          op(op.vertices, target.vertexSet).result.role
        }
        val parted = {
          val op = graph_operations.PartitionAttribute[Double]()
          op(op.attr, target)(op.role, roles).result
        }
        val prefix = params("prefix")
        parent.newVertexAttribute(s"${prefix}_roles", roles)
        parent.newVertexAttribute(s"${prefix}_${targetName}_test", parted.test)
        var train = parted.train.entity
        val segSizes = computeSegmentSizes(seg)
        project.newVertexAttribute("size", segSizes)
        val maxDeviation = params("max_deviation")

        val coverage = {
          val op = graph_operations.CountAttributes[Double]()
          op(op.attribute, train).result.count
        }
        parent.newVertexAttribute(s"${prefix}_${targetName}_train", train)
        parent.scalars(s"$prefix $targetName coverage initial") = coverage

        var timeOfDefinition = {
          val op = graph_operations.DeriveJSDouble(JavaScript("0"), Seq("attr"))
          op(op.attrs, graph_operations.VertexAttributeToJSValue.seq(train)).result.attr.entity
        }

        // iterative prediction
        for (i <- 1 to params("iterations").toInt) {
          val segTargetAvg = {
            aggregateViaConnection(
              seg.belongsTo,
              AttributeWithLocalAggregator(train, "average"))
              .runtimeSafeCast[Double]
          }
          val segStdDev = {
            aggregateViaConnection(
              seg.belongsTo,
              AttributeWithLocalAggregator(train, "std_deviation"))
              .runtimeSafeCast[Double]
          }
          val segTargetCount = {
            aggregateViaConnection(
              seg.belongsTo,
              AttributeWithLocalAggregator(train, "count"))
              .runtimeSafeCast[Double]
          }
          val segStdDevDefined = {
            val op = graph_operations.DeriveJSDouble(
              JavaScript(s"""
                deviation <= $maxDeviation &&
                defined / ids >= ${params("min_ratio_defined")} &&
                defined >= ${params("min_num_defined")}
                ? deviation
                : undefined"""),
              Seq("deviation", "ids", "defined"))
            op(
              op.attrs,
              graph_operations.VertexAttributeToJSValue.seq(segStdDev, segSizes, segTargetCount))
              .result.attr
          }
          project.newVertexAttribute(
            s"${prefix}_${targetName}_standard_deviation_after_iteration_$i",
            segStdDev)
          project.newVertexAttribute(
            s"${prefix}_${targetName}_average_after_iteration_$i",
            segTargetAvg)
          val predicted = {
            aggregateViaConnection(
              seg.belongsTo.reverse,
              AttributeWithWeightedAggregator(segStdDevDefined, segTargetAvg, "by_min_weight"))
              .runtimeSafeCast[Double]
          }
          train = unifyAttributeT(train, predicted)
          val partedTrain = {
            val op = graph_operations.PartitionAttribute[Double]()
            op(op.attr, train)(op.role, roles).result
          }
          val error = {
            val op = graph_operations.DeriveJSDouble(
              JavaScript("Math.abs(test - train)"), Seq("test", "train"))
            val mae = op(
              op.attrs,
              graph_operations.VertexAttributeToJSValue.seq(
                parted.test.entity, partedTrain.test.entity)).result.attr
            aggregate(AttributeWithAggregator(mae, "average"))
          }
          val coverage = {
            val op = graph_operations.CountAttributes[Double]()
            op(op.attribute, partedTrain.train).result.count
          }
          // the attribute we use for iteration can be defined on the test set as well
          parent.newVertexAttribute(s"${prefix}_${targetName}_after_iteration_$i", train)
          parent.scalars(s"$prefix $targetName coverage after iteration $i") = coverage
          parent.scalars(s"$prefix $targetName mean absolute prediction error after iteration $i") =
            error

          timeOfDefinition = {
            val op = graph_operations.DeriveJSDouble(
              JavaScript(i.toString), Seq("attr"))
            val newDefinitions = op(
              op.attrs, graph_operations.VertexAttributeToJSValue.seq(train)).result.attr
            unifyAttributeT(timeOfDefinition, newDefinitions)
          }
        }
        parent.newVertexAttribute(s"${prefix}_${targetName}_spread_over_iterations", timeOfDefinition)
        // TODO: in the end we should calculate with the fact that the real error where the
        // original attribute is defined is 0.0
      }
    })

  register("Correlate two attributes", GlobalOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("attrA", "First attribute", options = project.vertexAttrList[Double]),
      Choice("attrB", "Second attribute", options = project.vertexAttrList[Double]))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes")
    def apply() = {
      val attrA = project.vertexAttributes(params("attrA")).runtimeSafeCast[Double]
      val attrB = project.vertexAttributes(params("attrB")).runtimeSafeCast[Double]
      val op = graph_operations.CorrelateAttributes()
      val res = op(op.attrA, attrA)(op.attrB, attrB).result
      project.scalars(s"correlation of ${params("attrA")} and ${params("attrB")}") =
        res.correlation
    }
  })

  register("Filter by attributes", StructureOperations, new ProjectTransformation(_) {
    params ++= project.vertexAttrList.map {
      attr => Param(s"filterva_${attr.id}", attr.id)
    }
    params ++= project.segmentations.map {
      seg =>
        Param(
          s"filterva_${seg.viewer.equivalentUIAttributeTitle}",
          seg.segmentationName)
    }
    params ++= project.edgeAttrList.map {
      attr => Param(s"filterea_${attr.id}", attr.id)
    }
    def enabled =
      FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes") ||
        FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    val vaFilter = "filterva_(.*)".r
    val eaFilter = "filterea_(.*)".r

    override def summary = {
      val filterStrings = params.toMap.collect {
        case (vaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
        case (eaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
      }
      "Filter " + filterStrings.mkString(", ")
    }
    def apply() = {
      val vertexFilters = params.toMap.collect {
        case (vaFilter(name), filter) if filter.nonEmpty =>
          // The filter may be for a segmentation's equivalent attribute or for a vertex attribute.
          val segs = project.segmentations.map(_.viewer)
          val segGUIDOpt =
            segs.find(_.equivalentUIAttributeTitle == name).map(_.belongsToAttribute.gUID)
          val gUID = segGUIDOpt.getOrElse(project.vertexAttributes(name).gUID)
          FEVertexAttributeFilter(gUID.toString, filter)
      }.toSeq

      if (vertexFilters.nonEmpty) {
        val vertexEmbedding = FEFilters.embedFilteredVertices(
          project.vertexSet, vertexFilters, heavy = true)
        project.pullBack(vertexEmbedding)
      }
      val edgeFilters = params.toMap.collect {
        case (eaFilter(name), filter) if filter.nonEmpty =>
          val attr = project.edgeAttributes(name)
          FEVertexAttributeFilter(attr.gUID.toString, filter)
      }.toSeq
      assert(vertexFilters.nonEmpty || edgeFilters.nonEmpty, "No filters specified.")
      if (edgeFilters.nonEmpty) {
        val edgeEmbedding = FEFilters.embedFilteredVertices(
          project.edgeBundle.idSet, edgeFilters, heavy = true)
        project.pullBackEdges(edgeEmbedding)
      }
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

  register("Copy edges to segmentation", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    def enabled = project.assertSegmentation &&
      FEStatus.assert(parent.edgeBundle != null, "No edges on base project")
    def apply() = {
      val induction = {
        val op = graph_operations.InducedEdgeBundle()
        op(op.srcMapping, seg.belongsTo)(op.dstMapping, seg.belongsTo)(op.edges, parent.edgeBundle).result
      }
      project.edgeBundle = induction.induced
      for ((name, attr) <- parent.edgeAttributes) {
        project.edgeAttributes(name) = attr.pullVia(induction.embedding)
      }
    }
  })

  private def segmentationSizesProductSum(seg: SegmentationEditor, parent: ProjectEditor)(
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

  register("Copy edges to base project", StructureOperations, new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    override def visibleScalars =
      if (project.isSegmentation && project.edgeBundle != null) {
        val scalar = segmentationSizesProductSum(seg, parent)
        implicit val entityProgressManager = env.entityProgressManager
        List(ProjectViewer.feScalar(scalar, "num_copied_edges", "", Map()))
      } else {
        List()
      }
    def enabled = project.assertSegmentation &&
      project.hasEdgeBundle &&
      FEStatus.assert(parent.edgeBundle == null, "There are already edges on base project")
    def apply() = {
      val seg = project.asSegmentation
      val reverseBelongsTo = seg.belongsTo.reverse
      val induction = {
        val op = graph_operations.InducedEdgeBundle()
        op(op.srcMapping, reverseBelongsTo)(
          op.dstMapping, reverseBelongsTo)(
            op.edges, seg.edgeBundle).result
      }
      parent.edgeBundle = induction.induced
      for ((name, attr) <- seg.edgeAttributes) {
        parent.edgeAttributes(name) = attr.pullVia(induction.embedding)
      }
    }
  })

  register("Split to train and test set", MachineLearningOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("source", "Source attribute",
        options = project.vertexAttrList),
      Ratio("test_set_ratio", "Test set ratio", defaultValue = "0.1"),
      RandomSeed("seed", "Random seed for test set selection"))
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def apply() = {
      val sourceName = params("source")
      val source = project.vertexAttributes(sourceName)
      val roles = {
        val op = graph_operations.CreateRole(
          params("test_set_ratio").toDouble, params("seed").toInt)
        op(op.vertices, source.vertexSet).result.role
      }
      val testSetRatio = params("test_set_ratio").toDouble
      val parted = partitionVariable(source, roles)

      project.newVertexAttribute(s"${sourceName}_test", parted.test, s"ratio: $testSetRatio" + help)
      project.newVertexAttribute(s"${sourceName}_train", parted.train, s"ratio: ${1 - testSetRatio}" + help)
    }
    def partitionVariable[T](
      source: Attribute[T], roles: Attribute[String]): graph_operations.PartitionAttribute.Output[T] = {
      val op = graph_operations.PartitionAttribute[T]()
      op(op.attr, source)(op.role, roles).result
    }
  })

  register("Predict with a neural network (1st version)",
    MachineLearningOperations, new ProjectTransformation(_) {
      params ++= List(
        Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
        Param("output", "Save as"),
        Choice("features", "Predictors", options = FEOption.unset +: project.vertexAttrList[Double], multipleChoice = true),
        Choice("networkLayout", "Network layout", options = FEOption.list("GRU", "LSTM", "MLP")),
        NonNegInt("networkSize", "Size of the network", default = 3),
        NonNegInt("radius", "Iterations in prediction", default = 3),
        Choice("hideState", "Hide own state", options = FEOption.bools),
        NonNegDouble("forgetFraction", "Forget fraction", defaultValue = "0.0"),
        NonNegDouble("knownLabelWeight", "Weight for known labels", defaultValue = "1.0"),
        NonNegInt("numberOfTrainings", "Number of trainings", default = 50),
        NonNegInt("iterationsInTraining", "Iterations in training", default = 2),
        NonNegInt("subgraphsInTraining", "Subgraphs in training", default = 10),
        NonNegInt("minTrainingVertices", "Minimum training subgraph size", default = 10),
        NonNegInt("maxTrainingVertices", "Maximum training subgraph size", default = 20),
        NonNegInt("trainingRadius", "Radius for training subgraphs", default = 3),
        RandomSeed("seed", "Seed"),
        NonNegDouble("learningRate", "Learning rate", defaultValue = "0.1"))
      def enabled = project.hasEdgeBundle && FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No vertex attributes.")
      def apply() = {
        val labelName = params("label")
        val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
        val features: Seq[Attribute[Double]] =
          if (params("features") == FEOption.unset.id) Seq()
          else {
            val featureNames = splitParam("features")
            featureNames.map(name => project.vertexAttributes(name).runtimeSafeCast[Double])
          }
        val prediction = {
          val op = graph_operations.PredictViaNNOnGraphV1(
            featureCount = features.length,
            networkSize = params("networkSize").toInt,
            learningRate = params("learningRate").toDouble,
            radius = params("radius").toInt,
            hideState = params("hideState").toBoolean,
            forgetFraction = params("forgetFraction").toDouble,
            trainingRadius = params("trainingRadius").toInt,
            maxTrainingVertices = params("maxTrainingVertices").toInt,
            minTrainingVertices = params("minTrainingVertices").toInt,
            iterationsInTraining = params("iterationsInTraining").toInt,
            subgraphsInTraining = params("subgraphsInTraining").toInt,
            numberOfTrainings = params("numberOfTrainings").toInt,
            knownLabelWeight = params("knownLabelWeight").toDouble,
            seed = params("seed").toInt,
            gradientCheckOn = false,
            networkLayout = params("networkLayout"))
          op(op.edges, project.edgeBundle)(op.label, label)(op.features, features).result.prediction
        }
        project.vertexAttributes(params("output")) = prediction
      }
    })

  register("Lookup region", VertexAttributesOperations, new ProjectTransformation(_) {
    params ++= List(
      Choice("position", "Position", options = project.vertexAttrList[(Double, Double)]),
      Choice("shapefile", "Shapefile", options = listShapefiles(), allowUnknownOption = true),
      Param("attribute", "Attribute from the Shapefile"),
      Choice("ignoreUnsupportedShapes", "Ignore unsupported shape types",
        options = FEOption.boolsDefaultFalse),
      Param("output", "Output name"))
    def enabled = FEStatus.assert(
      project.vertexAttrList[(Double, Double)].nonEmpty, "No position vertex attributes.")

    def apply() = {
      val shapeFilePath = getShapeFilePath(params)
      val position = project.vertexAttributes(params("position")).runtimeSafeCast[(Double, Double)]
      val op = graph_operations.LookupRegion(
        shapeFilePath,
        params("attribute"),
        params("ignoreUnsupportedShapes").toBoolean)
      val result = op(op.coordinates, position).result
      project.newVertexAttribute(params("output"), result.attribute)
    }
  })

  register("Segment by geographical proximity", StructureOperations, new ProjectTransformation(_) {
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

  private def getShapeFilePath(params: ParameterHolder): String = {
    val shapeFilePath = params("shapefile")
    assert(listShapefiles().exists(f => f.id == shapeFilePath),
      "Shapefile deleted, please choose another.")
    shapeFilePath
  }

  private def listShapefiles(): List[FEOption] = {
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
  private def aggregate[From, Intermediate, To](
    attributeWithAggregator: AttributeWithAggregator[From, Intermediate, To]): Scalar[To] = {
    val op = graph_operations.AggregateAttributeToScalar(attributeWithAggregator.aggregator)
    op(op.attr, attributeWithAggregator.attr).result.aggregated
  }

  // Performs AggregateByEdgeBundle.
  private def aggregateViaConnection[From, To](
    connection: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateByEdgeBundle(attributeWithAggregator.aggregator)
    op(op.connection, connection)(op.attr, attributeWithAggregator.attr).result.attr
  }

  // Performs AggregateFromEdges.
  private def aggregateFromEdges[From, To](
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

  private def unifyAttributeT[T](a1: Attribute[T], a2: Attribute[_]): Attribute[T] = {
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
