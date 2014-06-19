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

  registerOperation(ExportCSV)
  object ExportCSV extends FEOperation {
    val title = "Export to CSV"
    val parameters = Seq(
      Param("path", "Destination path"),
      Param("v_labels", "Vertex labels (comma-separated)"),
      Param("v_attrs", "Vertex attributes", kind = "multi-vertex-attribute"),
      Param("e_labels", "Edge labels (comma-separated)"),
      Param("e_attrs", "Edge attributes", kind = "multi-edge-attribute"))

    private def split(s: String): Seq[String] =
      if (s.isEmpty) Nil else s.split(",")

    private def getAttributes(params: Map[String, String]) = {
      val vAttrs = split(params("v_attrs")).map(id => manager.vertexAttribute(id.asUUID))
      val eAttrs = split(params("e_attrs")).map(id => manager.edgeAttribute(id.asUUID))
      (vAttrs, eAttrs)
    }

    private def getLabels(params: Map[String, String]) =
      (split(params("v_labels")), split(params("e_labels")))

    def validate(params: Map[String, String]) = {
      val (vAttrs, eAttrs) = getAttributes(params)
      val vertexSets = vAttrs.map(_.vertexSet).toSet
      val edgeBundles = eAttrs.map(_.edgeBundle).toSet
      val (vLabels, eLabels) = getLabels(params)
      if (vLabels.size != vAttrs.size) FEStatus.failure("Wrong number of vertex labels.")
      else if (eLabels.size != eAttrs.size) FEStatus.failure("Wrong number of edge labels.")
      else if (vAttrs.isEmpty && eAttrs.isEmpty) FEStatus.failure("Nothing selected for export.")
      else if (vertexSets.size > 1) FEStatus.failure("All vertex attributes must be for the same vertex set.")
      else if (edgeBundles.size > 1) FEStatus.failure("All edge attributes must be for the same edge bundle.")
      else if (vertexSets.nonEmpty && edgeBundles.nonEmpty && (
        edgeBundles.head.srcVertexSet != vertexSets.head &&
        edgeBundles.head.dstVertexSet != vertexSets.head)) FEStatus.failure("The edge bundle is not local to the vertex set.")
      else FEStatus.success
    }

    def apply(params: Map[String, String]): FEStatus = {
      if (!validate(params).success) return validate(params)
      val (vLabels, eLabels) = getLabels(params)
      val (vAttrs, eAttrs) = getAttributes(params)
      val path = Filename.fromString(params("path"))
      if (vAttrs.nonEmpty)
        graph_util.CSVExport
          .exportVertexAttributes(vAttrs, vLabels, dataManager)
          .saveToDir(path / "vertices")
      if (eAttrs.nonEmpty)
        graph_util.CSVExport
          .exportEdgeAttributes(eAttrs, eLabels, dataManager)
          .saveToDir(path / "edges")
      return FEStatus.success
    }
  }
}
