package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.JavaScript
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
    val title = "New vertex set"
    val category = "Vertex operations"
    val parameters = List(
      Param("size", "Vertex set size"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.CreateVertexSet(params("size").toInt))
    }
  }

  registerOperation(RandomEdgeBundle)
  object RandomEdgeBundle extends FEOperation {
    val title = "Create random edge bundle"
    val category = "Edge operations"
    val parameters = List(
      Param("vsSrc", "Source vertex set", kind = "vertex-set"),
      Param("vsDst", "Destination vertex set", kind = "vertex-set"),
      Param("density", "density", defaultValue = "0.5"),
      Param("seed", "Seed", defaultValue = "0"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.SimpleRandomEdgeBundle(params("seed").toInt, params("density").toFloat),
        'vsSrc -> manager.vertexSet(params("vsSrc").asUUID),
        'vsDst -> manager.vertexSet(params("vsDst").asUUID))
    }
  }

  registerOperation(ImportVertices)
  object ImportVertices extends FEOperation {
    val title = "Import vertices"
    val category = "Vertex operations"
    val parameters = List(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("filter", "(optional) Filtering expression"))
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename(params("files")),
        params("delimiter"),
        params("header"),
        JavaScript(params("filter")))
      manager.show(graph_operations.ImportVertexList(csv))
    }
  }

  registerOperation(ImportEdges)
  object ImportEdges extends FEOperation {
    val title = "Import edges"
    val category = "Edge operations"
    val parameters = List(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"),
      Param("filter", "(optional) Filtering expression"))
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename(params("files")),
        params("delimiter"),
        params("header"),
        JavaScript(params("filter")))
      val src = params("src")
      val dst = params("dst")
      manager.show(graph_operations.ImportEdgeList(csv, src, dst))
    }
  }

  registerOperation(ImportEdgesForExistingVertexSet)
  object ImportEdgesForExistingVertexSet extends FEOperation {
    val title = "Import edges"
    val category = "Edge operations"
    val parameters = List(
      Param("vsSrc", "Source vertex id attributes", kind = "vertex-attribute"),
      Param("vsDst", "Destination vertex id attributes", kind = "vertex-attribute"),
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("srcField", "Source ID field"),
      Param("dstField", "Destination ID field "),
      Param("filter", "(optional) Filtering expression"))
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename(params("files")),
        params("delimiter"),
        params("header"),
        JavaScript(params("filter")))
      val srcField = params("srcField")
      val dstField = params("dstField")
      manager.show(
        graph_operations.ImportEdgeListForExistingVertexSet(csv, srcField, dstField),
        'srcVidAttr -> manager.vertexAttribute(params("vsSrc").asUUID),
        'dstVidAttr -> manager.vertexAttribute(params("vsDst").asUUID))
    }
  }

  registerOperation(FindMaxCliques)
  object FindMaxCliques extends FEOperation {
    val title = "Maximal cliques"
    val category = "Segmentations"
    val parameters = List(
      Param("es", "Edge bundle", kind = "edge-bundle"),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.FindMaxCliques(params("min").toInt),
        'es -> manager.edgeBundle(params("es").asUUID))
    }
  }

  registerOperation(SetOverlap)
  object SetOverlap extends FEOperation {
    val title = "Set overlaps (complete, regular)"
    val category = "Edge operations"
    val parameters = List(
      Param("belongsTo", "Edge bundle linking the input and its segments", kind = "edge-bundle"),
      Param("min", "Minimum overlap size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.SetOverlap(params("min").toInt),
        'belongsTo -> manager.edgeBundle(params("belongsTo").asUUID))
    }
  }

  registerOperation(UniformOverlapForCC)
  object UniformOverlapForCC extends FEOperation {
    val title = "Set overlaps (for connected components, regular)"
    val category = "Edge operations"
    val parameters = List(
      Param("belongsTo", "Edge bundle linking the input and its segments", kind = "edge-bundle"),
      Param("min", "Minimum overlap size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.UniformOverlapForCC(params("min").toInt),
        'belongsTo -> manager.edgeBundle(params("belongsTo").asUUID))
    }
  }

  registerOperation(InfocomOverlapForCC)
  object InfocomOverlapForCC extends FEOperation {
    val title = "Set overlaps (for connected components, infocom)"
    val category = "Edge operations"
    val parameters = List(
      Param("belongsTo", "Edge bundle linking the input and its groups", kind = "edge-bundle"),
      Param("thr", "Adjacency threshold of infocom overlap function", defaultValue = "0.6"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.InfocomOverlapForCC(params("thr").toDouble),
        'belongsTo -> manager.edgeBundle(params("belongsTo").asUUID))
    }
  }

  registerOperation(ConnectedComponents)
  object ConnectedComponents extends FEOperation {
    val title = "Connected components"
    val category = "Segmentations"
    val parameters = List(
      Param("es", "Edge bundle", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.ConnectedComponents(),
        'es -> manager.edgeBundle(params("es").asUUID))
    }
  }

  registerOperation(ConcatenateBundles)
  object ConcatenateBundles extends FEOperation {
    val title = "Concatenate edge bundles, weighted"
    val category = "X - Expert operations"
    val parameters = List(
      Param("wAB", "Edge weight A->B", kind = "edge-attribute"),
      Param("wBC", "Edge weight B->C", kind = "edge-attribute"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.ConcatenateBundles(),
        'weightsAB -> manager.vertexAttribute(params("wAB").asUUID),
        'weightsBC -> manager.vertexAttribute(params("wBC").asUUID))
    }
  }

  registerOperation(AddConstantDoubleEdgeAttribute)
  object AddConstantDoubleEdgeAttribute extends FEOperation {
    val title = "Add constant edge attribute"
    val category = "Attribute operations"
    val parameters = List(
      Param("eb", "Edge bundle", kind = "edge-bundle"),
      Param("v", "Value", defaultValue = "1"))
    def apply(params: Map[String, String]) = {
      val edges = manager.edgeBundle(params("eb").asUUID)
      import Scripting._
      val op = graph_operations.AddConstantDoubleAttribute(params("v").toDouble)
      val vertexAttr = op(op.vs, edges.idSet).result.attr
      manager.show(Seq(vertexAttr.entity))
    }
  }

  registerOperation(AddGaussianVertexAttribute)
  object AddGaussianVertexAttribute extends FEOperation {
    val title = "Add Gaussian vertex attribute"
    val category = "Attribute operations"
    val parameters = List(
      Param("vs", "Vertex set", kind = "vertex-set"),
      Param("seed", "Seed", defaultValue = "0"))
    def apply(params: Map[String, String]) = {
      val vertices = manager.vertexSet(params("vs").asUUID)
      import Scripting._
      val op = graph_operations.AddGaussianVertexAttribute(params("seed").toInt)
      manager.show(op(op.vertices, vertices).result.metaDataSet)
    }
  }

  registerOperation(ReverseEdges)
  object ReverseEdges extends FEOperation {
    val title = "Reverse edge direction"
    val category = "Edge operations"
    val parameters = List(
      Param("eb", "Edge bundle", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.ReverseEdges(),
        'esAB -> manager.edgeBundle(params("eb").asUUID))
    }
  }

  registerOperation(ClusteringCoefficient)
  object ClusteringCoefficient extends FEOperation {
    val title = "Clustering coefficient"
    val category = "Attribute operations"
    val parameters = List(
      Param("eb", "Edge bundle", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.ClusteringCoefficient(),
        'es -> manager.edgeBundle(params("eb").asUUID))
    }
  }

  registerOperation(OutDegree)
  object OutDegree extends FEOperation {
    val title = "Out degree"
    val category = "Attribute operations"
    val parameters = List(
      Param("w", "Edges", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.OutDegree(),
        'es -> manager.edgeBundle(params("w").asUUID))
    }
  }

  registerOperation(ExampleGraph)
  object ExampleGraph extends FEOperation {
    val title = "Example Graph"
    val category = "Vertex operations"
    val parameters = List()
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.ExampleGraph())
    }
  }

  registerOperation(AttributeConversion)
  object AttributeConversion extends FEOperation {
    val title = "Convert attributes"
    val category = "Attribute operations"
    val parameters = List(
      Param("vattrs", "Vertex attributes", kind = "multi-vertex-attribute"),
      Param("eattrs", "Edge attributes", kind = "multi-edge-attribute"),
      Param("type", "Convert into", options = UIValue.list(List("string", "double"))))

    def apply(params: Map[String, String]) = {
      val vAttrs: Seq[String] = if (params("vattrs").isEmpty) Nil else params("vattrs").split(",")
      val eAttrs: Seq[String] = if (params("eattrs").isEmpty) Nil else params("eattrs").split(",")
      val attrs = vAttrs ++ eAttrs
      val as = attrs.map(s => manager.vertexAttribute(s.asUUID))
      val typ = params("type")
      if (typ == "string") {
        val okAs = as.filter(!_.is[String])
        assert(okAs.nonEmpty, "Nothing to convert.")
        for (a <- okAs) manager.show(graph_operations.VertexAttributeToString(), 'attr -> a)
      } else if (typ == "double") {
        val okAs = as.filter(_.is[String])
        assert(okAs.nonEmpty, "Nothing to convert.")
        for (a <- okAs) manager.show(graph_operations.VertexAttributeToDouble(), 'attr -> a)
      } else assert(false, s"Unexpected type: $typ")
    }
  }

  registerOperation(AddReversedEdges)
  object AddReversedEdges extends FEOperation {
    val title = "Add reversed edges"
    val category = "Edge operations"
    val parameters = List(
      Param("es", "Edges", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.AddReversedEdges(),
        'es -> manager.edgeBundle(params("es").asUUID))
    }
  }

  registerOperation(EdgeGraph)
  object EdgeGraph extends FEOperation {
    val title = "Dual graph"
    val category = "Edge operations"
    val parameters = List(
      Param("es", "Edges", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.EdgeGraph(),
        'es -> manager.edgeBundle(params("es").asUUID))
    }
  }

  registerOperation(PageRank)
  object PageRank extends FEOperation {
    val title = "PageRank"
    val category = "Attribute operations"
    val parameters = List(
      Param("ws", "Weights", kind = "edge-attribute"),
      Param("df", "Damping factor"),
      Param("iter", "Iterations"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.PageRank(params("df").toDouble, params("iter").toInt),
        'weights -> manager.vertexAttribute(params("ws").asUUID))
    }
  }

  registerOperation(RemoveNonSymmetricEdges)
  object RemoveNonSymmetricEdges extends FEOperation {
    val title = "Remove non-symmetric edges"
    val category = "Edge operations"
    val parameters = List(
      Param("es", "Edges", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.RemoveNonSymmetricEdges(),
        'es -> manager.edgeBundle(params("es").asUUID))
    }
  }
}
