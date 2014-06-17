package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import scala.reflect.runtime.universe.typeOf

object FEOperations extends FEOperationRepository {
  val Param = FEOperationParameterMeta // Short alias.

  registerOperation(CreateVertexSet)
  object CreateVertexSet extends FEOperation {
    val title = "Create a new vertex set"
    val parameters = Seq(
      Param("size", "Vertex set size"))
    def instance(params: Map[String, String]) = manager.apply(
      graph_operations.CreateVertexSet(params("size").toInt))
  }

  registerOperation(RandomEdgeBundle)
  object RandomEdgeBundle extends FEOperation {
    val title = "Create a random edge bundle"
    val parameters = Seq(
      Param("vsSrc", "Source vertex set", kind = "vertex-set"),
      Param("vsDst", "Destination vertex set", kind = "vertex-set"),
      Param("density", "density", defaultValue = "0.5"),
      Param("seed", "Seed", defaultValue = "0"))
    def instance(params: Map[String, String]) = manager.apply(
      graph_operations.SimpleRandomEdgeBundle(params("seed").toInt, params("density").toFloat),
      'vsSrc -> manager.vertexSet(params("vsSrc").asUUID),
      'vsDst -> manager.vertexSet(params("vsDst").asUUID))
  }

  registerOperation(FindMaxCliques)
  object FindMaxCliques extends FEOperation {
    val title = "Find maximal cliques"
    val parameters = Seq(
      Param("vs", "Vertex set", kind = "vertex-set"),
      Param("es", "Edge bundle", kind = "edge-bundle"),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def instance(params: Map[String, String]) = manager.apply(
      graph_operations.FindMaxCliques(params("min").toInt),
      'vsIn -> manager.vertexSet(params("vs").asUUID),
      'esIn -> manager.edgeBundle(params("es").asUUID))
  }

  registerOperation(AddConstantDoubleEdgeAttribute)
  object AddConstantDoubleEdgeAttribute extends FEOperation {
    val title = "Add constant edge attribute"
    val parameters = Seq(
      Param("eb", "Edge bundle", kind = "edge-bundle"),
      Param("v", "Value", defaultValue = "1"))
    def instance(params: Map[String, String]) = {
      val edges = manager.edgeBundle(params("eb").asUUID)
      manager.apply(
        graph_operations.AddConstantDoubleEdgeAttribute(params("v").toDouble),
        'edges -> edges, 'ignoredSrc -> edges.srcVertexSet, 'ignoredDst -> edges.dstVertexSet)
    }
  }
}
