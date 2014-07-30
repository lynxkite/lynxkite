package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import scala.reflect.runtime.universe.typeOf

class Operations(env: BigGraphEnvironment) extends OperationRepository(env) {
  val Param = FEOperationParameterMeta // Short alias.

  register(new Operation(_) {
    val title = "Discard vertices"
    val category = "Vertex operations"
    val parameters = Seq()
    def enabled = if (project.vertexSet == null) FEStatus.failure("No vertices.") else FEStatus.success
    def apply(params: Map[String, String]) = {
      project.vertexSet = null
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Discard edges"
    val category = "Edge operations"
    val parameters = Seq()
    def enabled = if (project.edgeBundle == null) FEStatus.failure("No edges.") else FEStatus.success
    def apply(params: Map[String, String]) = {
      project.vertexSet = null
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "New vertex set"
    val category = "Vertex operations"
    val parameters = Seq(
      Param("size", "Vertex set size"))
    def enabled = if (project.vertexSet != null) FEStatus.failure("Vertices already exist.") else FEStatus.success
    def apply(params: Map[String, String]) = {
      val vs = graph_operations.CreateVertexSet(params("size").toInt)().result.vs
      project.vertexSet = vs
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Create random edge bundle"
    val category = "Edge operations"
    val parameters = Seq(
      Param("density", "density", defaultValue = "0.5"),
      Param("seed", "Seed", defaultValue = "0"))
    def enabled = if (project.edgeBundle != null) FEStatus.failure("Edges already exist.") else FEStatus.success
    def apply(params: Map[String, String]) = {
      val op = graph_operations.SimpleRandomEdgeBundle(params("seed").toInt, params("density").toFloat)
      project.edgeBundle = op(op.vsSrc, project.vertexSet)(op.vsDst, project.vertexSet).result.es
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Import vertices"
    val category = "Vertex operations"
    val parameters = Seq(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("filter", "(optional) Filtering expression"))
    def enabled = if (project.vertexSet != null) FEStatus.failure("Vertices already exist.") else FEStatus.success
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      val imp = graph_operations.ImportVertexList(csv)().result
      project.vertexSet = imp.vertices
      project.vertexAttributes = imp.attrs
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Import edges"
    val category = "Edge operations"
    val parameters = Seq(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"),
      Param("filter", "(optional) Filtering expression"))
    def enabled = if (project.edgeBundle != null) FEStatus.failure("Edges already exist.") else FEStatus.success
    def apply(project: Project, params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      val src = params("src")
      val dst = params("dst")
      if (project.vertexSet == null) {
        val imp = graph_operations.ImportEdgeList(csv, src, dst)().result
        project.vertexSet = imp.vertices
        project.edgeBundle = imp.edges
        project.edgeAttributes = imp.attrs
      } else {
        val op = graph_operations.ImportEdgeListForExistingVertexSet(csv, src, dst)
        val imp = op(op.sources, project.vertexSet)(op.destinations, project.vertexSet).result
        project.edgeBundle = imp.edges
        project.edgeAttributes = imp.attrs
      }
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Maximal cliques"
    val category = "Segmentations"
    val parameters = Seq(
      Param("name", "Segmentation name", defaultValue = "maximal_cliques"),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FindMaxCliques(params("min").toInt)
      project.segmentations(params("name")) = op(op.es, project.edgeBundle).result.segments
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Connected components"
    val category = "Segmentations"
    val parameters = Seq(
      Param("name", "Segmentation name", defaultValue = "connected_components"))
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ConnectedComponents()
      project.segmentations(params("name")) = op(op.es, project.edgeBundle).result.segments
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Add constant edge attribute"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"))
    def apply(params: Map[String, String]) = {
      val op = graph_operations.AddConstantDoubleEdgeAttribute(params("value").toDouble)
      project.edgeAttributes(params("name")) = op(op.edges, project.edgeBundle).result.attr
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Reverse edge direction"
    val category = "Edge operations"
    val parameters = Seq()
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ReverseEdges()
      project.edgeBundle = op(op.esAB, project.edgeBundle).result.esBA
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Clustering coefficient"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "clustering_coefficient"))
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ClusteringCoefficient()
      project.vertexAttributes(params("name")) = op(op.es, project.edgeBundle).result.clustering
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Weighted out degree"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "out_degree"),
      Param("w", "Weighted edges", options = project.edgeAttributeNames[Double]))
    def enabled = if (project.edgeAttributeNames[Double].isEmpty) FEStatus.failure("No numeric attribute.") else FEStatus.success
    def apply(params: Map[String, String]) = {
      val op = graph_operations.WeightedOutDegree()
      val deg = op(op.weights, manager.edgeAttribute(params("w").asUUID)).result.outDegree
      project.vertexAttributes(params("name")) = deg
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Example Graph"
    val category = "Vertex operations"
    val parameters = Seq()
    def enabled = if (project.vertexSet != null) FEStatus.failure("Vertices already exist.") else FEStatus.success
    def apply(params: Map[String, String]) = {
      val g = graph_operations.ExampleGraph()().result
      project.vertexSet = g.vertices
      project.edgeBundle = g.edges
      project.vertexAttributes = g.vertexAttributes
      project.edgeAttributes = g.edgeAttributes
      FEStatus.success
    }
  })

  register(new Operation(_) {
    val title = "Vertex attribute to string"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("attr", "Vertex attribute", options = project.vertexAttributeNames))
    def apply(params: Map[String, String]): FEStatus = {
      val attr = manager.vertexAttribute(params("attr").asTag)
      val op = graph_operations.VertexAttributeToString()
      project.vertexAttributes(params("attr")) = op(op.attr, attr).result.attr
      FEStatus.success
    }
  })
}
