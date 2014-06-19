package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import scala.reflect.runtime.universe.typeOf

class FEOperations(env: BigGraphEnvironment) extends FEOperationRepository(env) {
  val Param = FEOperationParameterMeta // Short alias.

  registerOperation(CreateVertexSet)
  object CreateVertexSet extends FEOperation {
    val title = "Create a new vertex set"
    val parameters = Seq(
      Param("size", "Vertex set size"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.CreateVertexSet(params("size").toInt))
      FEStatus.success
    }
  }

  registerOperation(RandomEdgeBundle)
  object RandomEdgeBundle extends FEOperation {
    val title = "Create a random edge bundle"
    val parameters = Seq(
      Param("vsSrc", "Source vertex set", kind = "vertex-set"),
      Param("vsDst", "Destination vertex set", kind = "vertex-set"),
      Param("density", "density", defaultValue = "0.5"),
      Param("seed", "Seed", defaultValue = "0"))
    def apply(params: Map[String, String]) = {
      manager.apply(
        graph_operations.SimpleRandomEdgeBundle(params("seed").toInt, params("density").toFloat),
        'vsSrc -> manager.vertexSet(params("vsSrc").asUUID),
        'vsDst -> manager.vertexSet(params("vsDst").asUUID))
      FEStatus.success
    }
  }

  registerOperation(FindMaxCliques)
  object FindMaxCliques extends FEOperation {
    val title = "Find maximal cliques"
    val parameters = Seq(
      Param("vs", "Vertex set", kind = "vertex-set"),
      Param("es", "Edge bundle", kind = "edge-bundle"),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.FindMaxCliques(params("min").toInt),
        'vsIn -> manager.vertexSet(params("vs").asUUID),
        'esIn -> manager.edgeBundle(params("es").asUUID))
      FEStatus.success
    }
  }

  registerOperation(AddConstantDoubleEdgeAttribute)
  object AddConstantDoubleEdgeAttribute extends FEOperation {
    val title = "Add constant edge attribute"
    val parameters = Seq(
      Param("eb", "Edge bundle", kind = "edge-bundle"),
      Param("v", "Value", defaultValue = "1"))
    def apply(params: Map[String, String]) = {
      val edges = manager.edgeBundle(params("eb").asUUID)
      manager.apply(
        graph_operations.AddConstantDoubleEdgeAttribute(params("v").toDouble),
        'edges -> edges, 'ignoredSrc -> edges.srcVertexSet, 'ignoredDst -> edges.dstVertexSet)
      FEStatus.success
    }
  }

  registerOperation(ExportCSVVertices)
  object ExportCSVVertices extends FEOperation {
    val title = "Export vertices to CSV"
    val parameters = Seq(
      Param("path", "Destination path"),
      Param("labels", "Labels (comma-separated)"),
      Param("attrs", "Attributes", kind = "multi-vertex-attribute"))

    def apply(params: Map[String, String]): FEStatus = {
      if (params("attrs").isEmpty || params("labels").isEmpty)
        return FEStatus.failure("Nothing selected for export.")
      val attrs = params("attrs").split(",").map(id => manager.vertexAttribute(id.asUUID))
      val labels = params("labels").split(",")
      if (labels.size != attrs.size)
        return FEStatus.failure("Wrong number of labels.")
      val vertexSets = attrs.map(_.vertexSet).toSet
      if (vertexSets.size != 1)
        return FEStatus.failure("All attributes must belong to the same vertex set.")
      val path = Filename.fromString(params("path"))
      if (path.isEmpty)
        return FEStatus.failure("No export path specified.")
      graph_util.CSVExport
        .exportVertexAttributes(attrs, labels, dataManager)
        .saveToDir(path)
      return FEStatus.success
    }
  }

  registerOperation(ExportCSVEdges)
  object ExportCSVEdges extends FEOperation {
    val title = "Export edges to CSV"
    val parameters = Seq(
      Param("path", "Destination path"),
      Param("labels", "Labels (comma-separated)"),
      Param("attrs", "Attributes", kind = "multi-edge-attribute"))

    def apply(params: Map[String, String]): FEStatus = {
      if (params("attrs").isEmpty || params("labels").isEmpty)
        return FEStatus.failure("Nothing selected for export.")
      val attrs = params("attrs").split(",").map(id => manager.edgeAttribute(id.asUUID))
      val labels = params("labels").split(",")
      if (labels.size != attrs.size)
        return FEStatus.failure("Wrong number of labels.")
      val edgeBundles = attrs.map(_.edgeBundle).toSet
      if (edgeBundles.size != 1)
        return FEStatus.failure("All attributes must belong to the same edge bundle.")
      val path = Filename.fromString(params("path"))
      if (path.isEmpty)
        return FEStatus.failure("No export path specified.")
      graph_util.CSVExport
        .exportEdgeAttributes(attrs, labels, dataManager)
        .saveToDir(path)
      return FEStatus.success
    }
  }
}
