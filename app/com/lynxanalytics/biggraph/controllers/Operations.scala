package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import scala.reflect.runtime.universe.typeOf

class Operations(env: BigGraphEnvironment) extends OperationRepository(env) {
  val Param = FEOperationParameterMeta // Short alias.

  // Categories.
  abstract class VertexOperation(p: Project) extends Operation(p, "Vertex operations")
  abstract class EdgeOperation(p: Project) extends Operation(p, "Edge operations")
  abstract class AttributeOperation(p: Project) extends Operation(p, "Attribute operations")
  abstract class SegmentationOperation(p: Project) extends Operation(p, "Segmentation operations")

  register(new VertexOperation(_) {
    val title = "Discard vertices"
    val parameters = Seq()
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      project.vertexSet = null
      FEStatus.success
    }
  })

  register(new EdgeOperation(_) {
    val title = "Discard edges"
    val parameters = Seq()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      project.edgeBundle = null
      FEStatus.success
    }
  })

  register(new VertexOperation(_) {
    val title = "New vertex set"
    val parameters = Seq(
      Param("size", "Vertex set size"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val vs = graph_operations.CreateVertexSet(params("size").toInt)().result.vs
      project.vertexSet = vs
      FEStatus.success
    }
  })

  register(new EdgeOperation(_) {
    val title = "Create random edge bundle"
    val parameters = Seq(
      Param("degree", "Average degree", defaultValue = "10"),
      Param("seed", "Seed", defaultValue = "0"))
    def enabled = hasVertexSet && hasNoEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FastRandomEdgeBundle(params("seed").toInt, params("degree").toInt)
      project.edgeBundle = op(op.vs, project.vertexSet).result.es
      FEStatus.success
    }
  })

  register(new EdgeOperation(_) {
    val title = "Connect vertices on attribute"
    val parameters = Seq(
      Param("attr", "Attribute", options = vertexAttributes[String]))
    def enabled =
      (hasVertexSet && hasNoEdgeBundle
        && FEStatus.assert(vertexAttributes[String].nonEmpty, "No string vertex attributes."))
    private def applyOn[T](attr: VertexAttribute[T]) = {
      val op = graph_operations.EdgesFromAttributeMatches[T]()
      project.edgeBundle = op(op.attr, attr).result.edges
      FEStatus.success
    }
    def apply(params: Map[String, String]) =
      applyOn(project.vertexAttributes(params("attr")))
  })

  register(new VertexOperation(_) {
    val title = "Import vertices"
    val parameters = Seq(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("filter", "(optional) Filtering expression"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      val imp = graph_operations.ImportVertexList(csv)().result
      project.vertexSet = imp.vertices
      project.vertexAttributes = imp.attrs.mapValues(_.entity)
      FEStatus.success
    }
  })

  register(new EdgeOperation(_) {
    val title = "Import edges for existing vertices"
    val parameters = Seq(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("attr", "Vertex id attribute", options = vertexAttributes[String]),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"),
      Param("filter", "(optional) Filtering expression"))
    def enabled =
      hasNoEdgeBundle &&
        hasVertexSet &&
        FEStatus.assert(vertexAttributes[String].nonEmpty, "No vertex attributes to use as id.")
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      val src = params("src")
      val dst = params("dst")
      val attr = project.vertexAttributes(params("attr")).runtimeSafeCast[String]
      val op = graph_operations.ImportEdgeListForExistingVertexSet(csv, src, dst)
      val imp = op(op.srcVidAttr, attr)(op.dstVidAttr, attr).result
      project.edgeBundle = imp.edges
      project.edgeAttributes = imp.attrs.mapValues(_.entity)
      FEStatus.success
    }
  })

  register(new EdgeOperation(_) {
    val title = "Import vertices and edges from single CSV fileset"
    val parameters = Seq(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"),
      Param("filter", "(optional) Filtering expression"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      val src = params("src")
      val dst = params("dst")
      val imp = graph_operations.ImportEdgeList(csv, src, dst)().result
      project.vertexSet = imp.vertices
      project.edgeBundle = imp.edges
      project.edgeAttributes = imp.attrs.mapValues(_.entity)
      FEStatus.success
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Maximal cliques"
    val parameters = Seq(
      Param("name", "Segmentation name", defaultValue = "maximal_cliques"),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FindMaxCliques(params("min").toInt)
      project.segmentations(params("name")) = op(op.es, project.edgeBundle).result.segments
      FEStatus.success
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Connected components"
    val parameters = Seq(
      Param("name", "Segmentation name", defaultValue = "connected_components"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ConnectedComponents()
      project.segmentations(params("name")) = op(op.es, project.edgeBundle).result.segments
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add gaussian vertex attribute"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "random"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val op = graph_operations.AddGaussianVertexAttribute()
      project.vertexAttributes(params("name")) = op(op.vertices, project.vertexSet).result.attr
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add constant edge attribute"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.AddConstantDoubleEdgeAttribute(params("value").toDouble)
      project.edgeAttributes(params("name")) = op(op.edges, project.edgeBundle).result.attr
      FEStatus.success
    }
  })

  register(new EdgeOperation(_) {
    val title = "Reverse edge direction"
    val parameters = Seq()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ReverseEdges()
      project.edgeBundle = op(op.esAB, project.edgeBundle).result.esBA
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Clustering coefficient"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "clustering_coefficient"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ClusteringCoefficient()
      project.vertexAttributes(params("name")) = op(op.es, project.edgeBundle).result.clustering
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted out degree"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "out_degree"),
      Param("w", "Weights", options = edgeAttributes[Double]))
    def enabled = FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes.")
    def apply(params: Map[String, String]) = {
      val op = graph_operations.WeightedOutDegree()
      val attr = project.edgeAttributes(params("w")).runtimeSafeCast[Double]
      val deg = op(op.attr, attr).result.outDegree
      project.vertexAttributes(params("name")) = deg
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "PageRank"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "page_rank"),
      Param("weights", "Weight attribute", options = edgeAttributes[Double]),
      Param("iterations", "Number of iterations", defaultValue = "5"),
      Param("damping", "Damping factor", defaultValue = "0.85"))
    def enabled = FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes.")
    def apply(params: Map[String, String]) = {
      val op = graph_operations.PageRank(params("damping").toDouble, params("iterations").toInt)
      val weights = project.edgeAttributes(params("weights")).runtimeSafeCast[Double]
      project.vertexAttributes(params("name")) = op(op.weights, weights).result.pagerank
      FEStatus.success
    }
  })

  register(new VertexOperation(_) {
    val title = "Example Graph"
    val parameters = Seq()
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val g = graph_operations.ExampleGraph()().result
      project.vertexSet = g.vertices
      project.edgeBundle = g.edges
      project.vertexAttributes = g.vertexAttributes.mapValues(_.entity)
      project.edgeAttributes = g.edgeAttributes.mapValues(_.entity)
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Vertex attribute to string"
    val parameters = Seq(
      Param("attr", "Vertex attribute", options = vertexAttributes))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
    private def applyOn[T](attr: VertexAttribute[T]) = {
      val op = graph_operations.VertexAttributeToString[T]()
      op(op.attr, attr).result.attr
    }
    def apply(params: Map[String, String]): FEStatus = {
      project.vertexAttributes(params("attr")) = applyOn(project.vertexAttributes(params("attr")))
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Vertex attribute to double"
    val parameters = Seq(
      Param("attr", "Vertex attribute", options = vertexAttributes[String]))
    def enabled = FEStatus.assert(vertexAttributes[String].nonEmpty, "No vertex attributes.")
    def apply(params: Map[String, String]): FEStatus = {
      val attr = project.vertexAttributes(params("attr")).runtimeSafeCast[String]
      val op = graph_operations.VertexAttributeToDouble()
      project.vertexAttributes(params("attr")) = op(op.attr, attr).result.attr
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Export vertex attributes to CSV"
    val parameters = Seq(
      Param("path", "Destination path"),
      Param("attrs", "Attributes", options = vertexAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
    def apply(params: Map[String, String]): FEStatus = {
      if (params("attrs").isEmpty)
        return FEStatus.failure("Nothing selected for export.")
      val labels = params("attrs").split(",")
      val attrs = labels.map(label => project.vertexAttributes(label))
      val path = Filename.fromString(params("path"))
      if (path.isEmpty)
        return FEStatus.failure("No export path specified.")
      graph_util.CSVExport
        .exportVertexAttributes(attrs, labels)
        .saveToDir(path)
      return FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Export edge attributes to CSV"
    val parameters = Seq(
      Param("path", "Destination path"),
      Param("attrs", "Attributes", options = edgeAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
    def apply(params: Map[String, String]): FEStatus = {
      if (params("attrs").isEmpty)
        return FEStatus.failure("Nothing selected for export.")
      val labels = params("attrs").split(",")
      val attrs = labels.map(label => project.edgeAttributes(label))
      val path = Filename.fromString(params("path"))
      if (path.isEmpty)
        return FEStatus.failure("No export path specified.")
      graph_util.CSVExport
        .exportEdgeAttributes(attrs, labels)
        .saveToDir(path)
      return FEStatus.success
    }
  })

  register(new VertexOperation(_) {
    val title = "Edge Graph"
    val parameters = Seq()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]): FEStatus = {
      val op = graph_operations.EdgeGraph()
      val g = op(op.es, project.edgeBundle).result
      project.vertexSet = g.newVS
      project.edgeBundle = g.newES
      return FEStatus.success
    }
  })
}
