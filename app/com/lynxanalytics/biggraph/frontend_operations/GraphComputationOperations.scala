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
        options = FEOption.list("Harmonic", "Lin", "Average distance", "Betweenness")),
      Choice("direction", "Direction",
        options = Direction.attrOptionsWithDefault("outgoing edges")),
      NonNegInt("maxDiameter", "Maximal diameter to check",
        default = 10, group = "Advanced settings"),
      NonNegInt("bits", "Precision", default = 8, group = "Advanced settings"))
    def enabled = project.hasEdgeBundle
    override def summary = {
      val variant = params("algorithm") match {
        case "Lin" => "Lin"
        case x => x.toLowerCase
      }
      s"Compute $variant centrality"
    }
    def apply() = {
      val name = params("name")
      val algorithm = params("algorithm")
      assert(name.nonEmpty, "Please set an attribute name.")
      val es = Direction(
        params("direction"),
        project.edgeBundle, reversed = true).edgeBundle
      val centrality: Attribute[Double] = algorithm match {
        case "Betweenness" =>
          graph_operations.NetworKitComputeAttribute.run("betweenness", es)
        case _ =>
          val op = graph_operations.HyperBallCentrality(
            params("maxDiameter").toInt, algorithm, params("bits").toInt)
          op(op.es, es).result.centrality
      }
      project.newVertexAttribute(name, centrality, algorithm + help)
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
