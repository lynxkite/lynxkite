// Frontend operations for building the base graph without segmentations.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._

class BuildGraphOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Implicits._

  val category = Categories.BuildGraphOperations

  import OperationParams._

  register("Connect vertices on attribute", List(projectInput))(new ProjectTransformation(_) {
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

  registerProjectCreatingOp("Create example graph")(new ProjectOutputOperation(_) {
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

  register("Add popularity x similarity optimized edges")(new ProjectTransformation(_) {
    params ++= List(
      NonNegDouble("externaldegree", "External degree", defaultValue = "1.5"),
      NonNegDouble("internaldegree", "Internal degree", defaultValue = "1.5"),
      NonNegDouble("exponent", "Exponent", defaultValue = "0.6"),
      RandomSeed("seed", "Seed"))
    def enabled = FEStatus.enabled
    def apply() = {
      val result = {
        val op = graph_operations.PSOGenerator(
          params("externaldegree").toDouble,
          params("internaldegree").toDouble,
          params("exponent").toDouble,
          params("seed").toLong)
        op(op.vs, project.vertexSet).result
      }
      project.newVertexAttribute("radial", result.radial)
      project.newVertexAttribute("angular", result.angular)
      project.edgeBundle = result.es
    }
  })

  register("Create random edges", List(projectInput))(new ProjectTransformation(_) {
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

  register("Create scale-free random edges", List(projectInput))(new ProjectTransformation(_) {
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

  registerProjectCreatingOp("Create vertices")(new ProjectOutputOperation(_) {
    params += NonNegInt("size", "Vertex set size", default = 10)
    def enabled = FEStatus.enabled
    def apply() = {
      val result = graph_operations.CreateVertexSet(params("size").toLong)().result
      project.setVertexSet(result.vs, idAttr = "id")
      project.newVertexAttribute("ordinal", result.ordinal)
    }
  })

  register("Predict edges with hyperbolic positions",
    List(projectInput))(new ProjectTransformation(_) {
      params ++= List(
        NonNegInt("size", "Number of predictions", default = 100),
        NonNegDouble("externaldegree", "External degree", defaultValue = "1.5"),
        NonNegDouble("internaldegree", "Internal degree", defaultValue = "1.5"),
        NonNegDouble("exponent", "Exponent", defaultValue = "0.6"),
        Choice("radial", "Radial coordinate",
          options = FEOption.unset +: project.vertexAttrList[Double]),
        Choice("angular", "Angular coordinate",
          options = FEOption.unset +: project.vertexAttrList[Double]))
      def enabled = FEStatus.assert(
        project.vertexAttrList[Double].size >= 2, "Not enough vertex attributes.")
      def apply() = {
        val op = graph_operations.HyperbolicPrediction(
          params("size").toInt,
          params("externaldegree").toDouble,
          params("internaldegree").toDouble,
          params("exponent").toDouble)
        val radAttr = project.vertexAttributes(params("radial"))
        val angAttr = project.vertexAttributes(params("angular"))
        assert(params("radial") != FEOption.unset.id, "The radial parameter must be set.")
        assert(params("angular") != FEOption.unset.id, "The angular parameter must be set.")
        val result = op(op.vs, project.vertexSet)(
          op.radial, radAttr.runtimeSafeCast[Double])(
            op.angular, angAttr.runtimeSafeCast[Double]).result
        if (project.hasEdgeBundle.enabled) {
          val oldBundle = project.edgeBundle
          project.edgeBundle = result.predictedEdges
          project.newEdgeAttribute("hyperbolic_edge_probability", result.edgeProbability,
            "hyperbolic edge probability")
          project.edgeBundle = generalEdgeBundleUnion(oldBundle, project.edgeBundle)
        } else {
          project.edgeBundle = result.predictedEdges
          project.newEdgeAttribute("hyperbolic_edge_probability", result.edgeProbability,
            "hyperbolic edge probability")
        }
      }
    })

  register(
    "Use table as vertices", List("table"))(factory = new ProjectOutputOperation(_) {
      lazy val vertices = tableLikeInput("table").asProject
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

  register(
    "Use table as graph", List("table"))(new ProjectOutputOperation(_) {
      lazy val edges = tableLikeInput("table").asProject
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

  register(
    "Use table as edges", List(projectInput, "table"))(new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val edges = tableLikeInput("table").asProject
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
}
