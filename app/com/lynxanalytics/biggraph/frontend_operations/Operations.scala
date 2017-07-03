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
