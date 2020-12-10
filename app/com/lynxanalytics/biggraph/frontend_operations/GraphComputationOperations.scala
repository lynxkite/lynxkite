package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.json

class GraphComputationOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Implicits._

  val category = Categories.GraphComputationOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Approximate clustering coefficient")(new ProjectTransformation(_) {
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

  register("Approximate embeddedness")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "embeddedness"),
      NonNegInt("bits", "Precision", default = 8))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.ApproxEmbeddedness(params("bits").toInt)
      project.newEdgeAttribute(params("name"), op(op.es, project.edgeBundle).result.embeddedness, help)
    }
  })

  register("Compute centrality")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "centrality"),
      Choice("algorithm", "Centrality type",
        options = FEOption.list(Seq(
          // Implemented on Spark.
          "Harmonic", "Lin", "Average distance",
          // From NetworKit.
          "Closeness (estimate)", "Betweenness (estimate)", "Laplacian",
          "Betweenness", "Eigenvector", "Harmonic Closeness", "Katz",
          "K-Path", "Sfigality").sorted.toList)),
      Choice("direction", "Direction",
        options = Direction.attrOptionsWithDefault("outgoing edges")),
      Choice("weight", "Edge weight",
        options = FEOption.list("No weighting") ++ project.edgeAttrList[Double]),
      NonNegInt("samples", "Sample size",
        default = 1000, group = "Advanced settings"),
      NonNegInt("maxDiameter", "Maximal diameter to check",
        default = 10, group = "Advanced settings"),
      NonNegInt("bits", "Precision", default = 8, group = "Advanced settings"))
    def enabled = project.hasEdgeBundle
    override def summary = s"Compute ${params("algorithm")} centrality"
    def apply() = {
      val name = params("name")
      val algorithm = params("algorithm")
      assert(name.nonEmpty, "Please set an attribute name.")
      val es = Direction(
        params("direction"),
        project.edgeBundle, reversed = true).edgeBundle
      val weight =
        if (params("weight") == "No weighting") None
        else Some(project.edgeAttributes(params("weight")).runtimeSafeCast[Double])
      def nk(algo: String) = graph_operations.NetworKitComputeAttribute.run(
        algo, es, Map("samples" -> params("samples").toInt), weight)
      val centrality: Attribute[Double] = algorithm match {
        case "Closeness (estimate)" => nk("ApproxCloseness")
        case "Betweenness" => nk("Betweenness")
        case "Eigenvector" => nk("EigenvectorCentrality")
        case "Betweenness (estimate)" => nk("EstimateBetweenness")
        case "Harmonic Closeness" => nk("HarmonicCloseness")
        case "Katz" => nk("KatzCentrality")
        case "K-Path" => nk("KPathCentrality")
        case "Laplacian" => nk("LaplacianCentrality")
        case "Sfigality" => nk("Sfigality")
        case _ =>
          val op = graph_operations.HyperBallCentrality(
            params("maxDiameter").toInt, algorithm, params("bits").toInt)
          op(op.es, es).result.centrality
      }
      project.newVertexAttribute(name, centrality, algorithm + help)
    }
  })

  register("Find k-core decomposition")(new ProjectTransformation(_) {
    params ++= List(Param("name", "Attribute name", defaultValue = "core"))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val name = params("name")
      // A directed graph just gets turned into an undirected graph.
      // We can skip that and just build an undirected graph.
      val core = graph_operations.NetworKitComputeAttribute.run(
        "CoreDecomposition", project.edgeBundle, Map("directed" -> false))
      project.newVertexAttribute(name, core, help)
    }
  })

  register("Place vertices with edge lengths")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "New attribute name", defaultValue = "position"),
      NonNegInt("dimensions", "Dimensions", default = 2),
      Choice("length", "Edge length",
        options = FEOption.list("Unit length") ++ project.edgeAttrList[Double]),
      Choice("algorithm", "Layout algorithm",
        options = FEOption.list("Pivot MDS", "Maxent-Stress")),
      NonNegInt("pivots", "Pivots", default = 100, group = "Pivot MDS options"),
      NonNegInt("radius", "Neighborhood radius", default = 1, group = "Maxent-Stress options"),
      NonNegDouble("tolerance", "Solver tolerance", defaultValue = "0.1", group = "Maxent-Stress options"))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val name = params("name")
      val weight =
        if (params("length") == "Unit length") None
        else Some(project.edgeAttributes(params("length")).runtimeSafeCast[Double])
      val positions = params("algorithm") match {
        case "Pivot MDS" => graph_operations.NetworKitComputeVectorAttribute.run(
          "PivotMDS", project.edgeBundle, Map(
            "directed" -> false,
            "dimensions" -> params("dimensions").toInt,
            "pivots" -> params("pivots").toInt), weight)
        case "Maxent-Stress" => graph_operations.NetworKitComputeVectorAttribute.run(
          "MaxentStress", project.edgeBundle, Map(
            "directed" -> false,
            "dimensions" -> params("dimensions").toInt,
            "radius" -> params("radius").toInt,
            "tolerance" -> params("tolerance").toDouble), weight)
      }
      project.newVertexAttribute(name, positions, help)
    }
  })

  register("Compute diameter")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Save as", defaultValue = "diameter"),
      NonNegDouble("max_error", "Maximum relative error", defaultValue = "0.1"))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val maxError = params("max_error").toDouble
      val result = graph_operations.NetworKitComputeScalar.run(
        "Diameter", project.edgeBundle, Map("directed" -> false, "max_error" -> maxError))
      val name = params("name")
      if (maxError == 0) {
        project.newScalar(name, result.scalar1)
      } else {
        project.newScalar(name + "_lower", result.scalar1)
        project.newScalar(name + "_upper", result.scalar2)
      }
    }
  })

  register("Compute effective diameter")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Save as", defaultValue = "effective diameter"),
      NonNegDouble("ratio", "Ratio to cover", defaultValue = "0.9"),
      Choice("algorithm", "Algorithm", options = FEOption.list("estimate", "exact")),
      NonNegInt("bits", "Extra bits", default = 7, group = "Esimation settings"),
      NonNegInt("approximations", "Number of parallel approximations", default = 64,
        group = "Esimation settings"))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val opts = Map("directed" -> false, "ratio" -> params("ratio").toDouble)
      val result = params("algorithm") match {
        case "exact" => graph_operations.NetworKitComputeScalar.run(
          "EffectiveDiameter", project.edgeBundle, opts)
        case "estimate" => graph_operations.NetworKitComputeScalar.run(
          "EffectiveDiameterApproximation", project.edgeBundle, opts ++ Map(
            "bits" -> params("bits").toInt,
            "approximations" -> params("approximations").toInt))
      }
      project.newScalar(params("name"), result.scalar1)
    }
  })

  register("Compute clustering coefficient")(new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "clustering_coefficient")
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.ClusteringCoefficient()
      project.newVertexAttribute(
        params("name"), op(op.es, project.edgeBundle).result.clustering, help)
    }
  })

  register("Compute degree")(new ProjectTransformation(_) {
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

  register("Compute dispersion")(new ProjectTransformation(_) {
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
        graph_operations.DeriveScala.derive[Double](
          "math.pow(disp, 0.61) / (emb + 5)",
          Seq("disp" -> dispersion, "emb" -> embeddedness))
      }
      // TODO: recursive dispersion
      project.newEdgeAttribute(params("name"), dispersion, help)
      project.newEdgeAttribute("normalized_" + params("name"), normalizedDispersion, help)
    }
  })

  register("Compute embeddedness")(new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "embeddedness")
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.Embeddedness()
      project.newEdgeAttribute(params("name"), op(op.es, project.edgeBundle).result.embeddedness, help)
    }
  })

  register("Compute hyperbolic edge probability")(new ProjectTransformation(_) {
    params ++= List(
      Choice("radial", "Radial coordinate",
        options = FEOption.unset +: project.vertexAttrList[Double]),
      Choice("angular", "Angular coordinate",
        options = FEOption.unset +: project.vertexAttrList[Double]))
    def enabled = project.hasEdgeBundle && FEStatus.assert(
      project.vertexAttrList[Double].size >= 2, "Not enough vertex attributes.")
    def apply() = {
      val result = {
        val degree = {
          val op = graph_operations.OutDegree()
          op(op.es, project.edgeBundle).result.outDegree
        }
        val clus = {
          val op = graph_operations.ApproxClusteringCoefficient(8)
          op(op.vs, project.vertexSet)(
            op.es, project.edgeBundle).result.clustering
        }
        assert(params("radial") != FEOption.unset.id, "The radial parameter must be set.")
        assert(params("angular") != FEOption.unset.id, "The angular parameter must be set.")
        val radAttr = project.vertexAttributes(params("radial"))
        val angAttr = project.vertexAttributes(params("angular"))
        val op = graph_operations.HyperbolicEdgeProbability()
        op(
          op.vs, project.vertexSet)(
            op.es, project.edgeBundle)(
              op.radial, radAttr.runtimeSafeCast[Double])(
                op.angular, angAttr.runtimeSafeCast[Double])(
                  op.degree, degree)(
                    op.clustering, clus).result
      }
      project.newEdgeAttribute("hyperbolic_edge_probability", result.edgeProbability,
        "hyperbolic edge probability")
    }
  })

  register("Compute PageRank")(new ProjectTransformation(_) {
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
      val direction = Direction(
        params("direction"),
        project.edgeBundle, reversed = true)
      val es = direction.edgeBundle
      val weights =
        if (weightsName == FEOption.noWeight.id) es.const(1.0)
        else direction.pull(project.edgeAttributes(params("weights"))).runtimeSafeCast[Double]
      project.newVertexAttribute(
        params("name"), op(op.es, es)(op.weights, weights).result.pagerank, help)
    }
  })

  register("Find Steiner tree")(new ProjectTransformation(_) {
    params ++= List(
      Param("ename", "Output edge attribute name", defaultValue = "arc"),
      Param("vname", "Output vertex attribute name", defaultValue = "node"),
      Param("pname", "Output graph attribute name for profit", defaultValue = "profit"),
      Param("rname", "Output vertex attribute name for the solution root points",
        defaultValue = "rootpoints"),
      Choice("edge_costs", "Cost attribute", options = project.edgeAttrList[Double]),
      Choice("root_costs", "Cost for using the point as root",
        options = project.vertexAttrList[Double]),
      Choice("gain", "Reward for reaching the vertex", options = project.vertexAttrList[Double]))

    def enabled = project.hasEdgeBundle &&
      FEStatus.assert(
        project.vertexAttrList[Double].size >= 2,
        "At least two numeric vertex attributes are needed.") &&
        FEStatus.assert(
          project.edgeAttrList[Double].size >= 1,
          "At least one numeric edge attribute is needed.")
    def apply() = {
      assert(params("ename").nonEmpty, "Please set an edge attribute name for the result")
      assert(params("vname").nonEmpty, "Please set a vertex attribute name for the result")
      assert(params("pname").nonEmpty, "Please set a name for the profit variable")
      assert(params("rname").nonEmpty, "Please set a name for the root points attribute for the result")

      val costAttr = project.edgeAttributes(params("edge_costs")).runtimeSafeCast[Double]
      val rootCostAttr = project.vertexAttributes(params("root_costs")).runtimeSafeCast[Double]
      val gain = project.vertexAttributes(params("gain")).runtimeSafeCast[Double]
      val es = project.edgeBundle
      val op = graph_operations.Dapcstp()
      val result =
        op(op.es, es)(op.edge_costs, costAttr)(op.root_costs, rootCostAttr)(op.gain, gain).result
      project.newEdgeAttribute(params("ename"), result.arcs)
      project.newVertexAttribute(params("vname"), result.nodes)
      project.newScalar(params("pname"), result.profit)
      project.newVertexAttribute(params("rname"), result.roots)

    }
  })

  register("Compute distance via shortest path")(new ProjectTransformation(_) {
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

  register("Find vertex coloring")(new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "color")
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.Coloring()
      project.newVertexAttribute(
        params("name"), op(op.es, project.edgeBundle).result.coloring, help)
    }
  })

  register("Fingerprint based on attributes")(new ProjectTransformation(_) {
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

  register("Map hyperbolic coordinates")(new ProjectTransformation(_) {
    params ++= List(
      RandomSeed("seed", "Seed", context.box))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val result = {
        val direction = Direction("all neighbors", project.edgeBundle)
        val degree = {
          val op = graph_operations.OutDegree()
          op(op.es, direction.edgeBundle).result.outDegree
        }
        val clus = {
          val op = graph_operations.ApproxClusteringCoefficient(8)
          op(op.vs, project.vertexSet)(
            op.es, direction.edgeBundle).result.clustering
        }
        val op = graph_operations.HyperMap(params("seed").toLong)
        op(
          op.vs, project.vertexSet)(
            op.es, direction.edgeBundle)(
              op.degree, degree)(
                op.clustering, clus).result
      }
      project.newVertexAttribute("radial", result.radial)
      project.newVertexAttribute("angular", result.angular)
    }
  })
}
