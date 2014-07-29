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
    val title = "New vertex set"
    val category = "Vertex operations"
    val parameters = Seq(
      Param("size", "Vertex set size"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.CreateVertexSet(params("size").toInt))
      FEStatus.success
    }
  }

  registerOperation(RandomEdgeBundle)
  object RandomEdgeBundle extends FEOperation {
    val title = "Create random edge bundle"
    val category = "Edge operations"
    val parameters = Seq(
      Param("vsSrc", "Source vertex set", kind = "vertex-set"),
      Param("vsDst", "Destination vertex set", kind = "vertex-set"),
      Param("density", "density", defaultValue = "0.5"),
      Param("seed", "Seed", defaultValue = "0"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.SimpleRandomEdgeBundle(params("seed").toInt, params("density").toFloat),
        'vsSrc -> manager.vertexSet(params("vsSrc").asUUID),
        'vsDst -> manager.vertexSet(params("vsDst").asUUID))
      FEStatus.success
    }
  }

  registerOperation(ImportVertices)
  object ImportVertices extends FEOperation {
    val title = "Import vertices"
    val category = "Vertex operations"
    val parameters = Seq(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("id", "(optional) Numeric field to use as ID"),
      Param("filter", "(optional) Filtering expression"))
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      manager.show(params("id") match {
        case "" => graph_operations.ImportVertexListWithStringIDs(csv)
        case id => graph_operations.ImportVertexListWithNumericIDs(csv, id)
      })
      FEStatus.success
    }
  }

  registerOperation(ImportEdges)
  object ImportEdges extends FEOperation {
    val title = "Import edges"
    val category = "Edge operations"
    val parameters = Seq(
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"),
      Param("id-type", "ID type", options = UIValue.seq("number", "string"), defaultValue = "number"),
      Param("filter", "(optional) Filtering expression"))
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      val src = params("src")
      val dst = params("dst")
      params("id-type") match {
        case "number" => manager.show(graph_operations.ImportEdgeListWithNumericIDs(csv, src, dst))
        case "string" => manager.show(graph_operations.ImportEdgeListWithStringIDs(csv, src, dst))
      }
      FEStatus.success
    }
  }

  registerOperation(ImportEdgesForExistingVertexSet)
  object ImportEdgesForExistingVertexSet extends FEOperation {
    val title = "Import edges"
    val category = "Edge operations"
    val parameters = Seq(
      Param("vsSrc", "Source vertex set", kind = "vertex-set"),
      Param("vsDst", "Destination vertex set", kind = "vertex-set"),
      Param("files", "Files"),
      Param("header", "Header"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("srcField", "Source ID field (numeric)"),
      Param("dstField", "Destination ID field (numeric)"),
      Param("filter", "(optional) Filtering expression"))
    def apply(params: Map[String, String]) = {
      val csv = graph_operations.CSV(
        Filename.fromString(params("files")),
        params("delimiter"),
        params("header"),
        graph_operations.Javascript(params("filter")))
      val srcField = params("srcField")
      val dstField = params("dstField")
      manager.show(
        graph_operations.ImportEdgeListWithNumericIDsForExistingVertexSet(csv, srcField, dstField),
        'sources -> manager.vertexSet(params("vsSrc").asUUID),
        'destinations -> manager.vertexSet(params("vsDst").asUUID))
      FEStatus.success
    }
  }

  registerOperation(FindMaxCliques)
  object FindMaxCliques extends FEOperation {
    val title = "Maximal cliques"
    val category = "Segmentations"
    val parameters = Seq(
      Param("es", "Edge bundle", kind = "edge-bundle"),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.FindMaxCliques(params("min").toInt),
        'esIn -> manager.edgeBundle(params("es").asUUID))
      FEStatus.success
    }
  }

  registerOperation(SetOverlap)
  object SetOverlap extends FEOperation {
    val title = "Set overlaps (complete, regular)"
    val category = "Edge operations"
    val parameters = Seq(
      Param("links", "Edge bundle linking the input and its groups", kind = "edge-bundle"),
      Param("min", "Minimum overlap size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.SetOverlap(params("min").toInt),
        'links -> manager.edgeBundle(params("links").asUUID))
      FEStatus.success
    }
  }

  registerOperation(UniformOverlapForCC)
  object UniformOverlapForCC extends FEOperation {
    val title = "Set overlaps (for connected components, regular)"
    val category = "Edge operations"
    val parameters = Seq(
      Param("links", "Edge bundle linking the input and its groups", kind = "edge-bundle"),
      Param("min", "Minimum overlap size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.UniformOverlapForCC(params("min").toInt),
        'links -> manager.edgeBundle(params("links").asUUID))
      FEStatus.success
    }
  }

  registerOperation(InfocomOverlapForCC)
  object InfocomOverlapForCC extends FEOperation {
    val title = "Set overlaps (for connected components, infocom)"
    val category = "Edge operations"
    val parameters = Seq(
      Param("links", "Edge bundle linking the input and its groups", kind = "edge-bundle"),
      Param("thr", "Adjacency threshold of infocom overlap function", defaultValue = "0.6"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.InfocomOverlapForCC(params("thr").toDouble),
        'links -> manager.edgeBundle(params("links").asUUID))
      FEStatus.success
    }
  }

  registerOperation(ConnectedComponents)
  object ConnectedComponents extends FEOperation {
    val title = "Connected components"
    val category = "Segmentations"
    val parameters = Seq(
      Param("es", "Edge bundle", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.ConnectedComponents(),
        'es -> manager.edgeBundle(params("es").asUUID))
      FEStatus.success
    }
  }

  registerOperation(ConcatenateBundles)
  object ConcatenateBundles extends FEOperation {
    val title = "Concatenate edge bundles, weighted"
    val category = "Expert"
    val parameters = Seq(
      Param("wAB", "Edge weight A->B", kind = "edge-attribute"),
      Param("wBC", "Edge weight B->C", kind = "edge-attribute"))
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.ConcatenateBundles(),
        'weightsAB -> manager.edgeAttribute(params("wAB").asUUID),
        'weightsBC -> manager.edgeAttribute(params("wBC").asUUID))
      FEStatus.success
    }
  }

  registerOperation(AddConstantDoubleEdgeAttribute)
  object AddConstantDoubleEdgeAttribute extends FEOperation {
    val title = "Add constant edge attribute"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("eb", "Edge bundle", kind = "edge-bundle"),
      Param("v", "Value", defaultValue = "1"))
    def apply(params: Map[String, String]) = {
      val edges = manager.edgeBundle(params("eb").asUUID)
      manager.show(
        graph_operations.AddConstantDoubleEdgeAttribute(params("v").toDouble),
        'edges -> edges, 'ignoredSrc -> edges.srcVertexSet, 'ignoredDst -> edges.dstVertexSet)
      FEStatus.success
    }
  }

  registerOperation(ReverseEdges)
  object ReverseEdges extends FEOperation {
    val title = "Reverse edge direction"
    val category = "Edge operations"
    val parameters = Seq(
      Param("eb", "Edge bundle", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.ReverseEdges(),
        'esAB -> manager.edgeBundle(params("eb").asUUID))
      FEStatus.success
    }
  }

  registerOperation(ClusteringCoefficient)
  object ClusteringCoefficient extends FEOperation {
    val title = "Clustering coefficient"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("eb", "Edge bundle", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.ClusteringCoefficient(),
        'es -> manager.edgeBundle(params("eb").asUUID))
      FEStatus.success
    }
  }

  registerOperation(WeightedOutDegree)
  object WeightedOutDegree extends FEOperation {
    val title = "Weighted out degree"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("w", "Weighted edges", kind = "edge-attribute"))
    def apply(params: Map[String, String]) = {
      manager.show(
        graph_operations.WeightedOutDegree(),
        'weights -> manager.edgeAttribute(params("w").asUUID))
      FEStatus.success
    }
  }

  registerOperation(ExportCSVVertices)
  object ExportCSVVertices extends FEOperation {
    val title = "Export vertex attributes to CSV"
    val category = "Attribute operations"
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
    val title = "Export edge attributes to CSV"
    val category = "Attribute operations"
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

  registerOperation(UpperBoundFilter)
  object UpperBoundFilter extends FEOperation {
    val title = "Filter by Vertex Attribute"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("attr", "Vertex Attribute", kind = "vertex-attribute"),
      Param("max", "Upper bound"))

    def apply(params: Map[String, String]) = {
      val attr = manager.vertexAttribute(params("attr").asUUID).runtimeSafeCast[Double]
      val orig = attr.vertexSet
      val edgeBundles = (manager.incomingBundles(orig) ++ manager.outgoingBundles(orig))
      val filter = manager.show(
        graph_operations.UpperBoundFilter(params("max").toDouble),
        'attr -> attr)
      val filtered = filter.outputs.vertexSets('fvs)
      // Filter all the visible edge bundles too.
      for (eb <- edgeBundles.filter(manager.isVisible(_))) {
        def f(vs: VertexSet) = if (vs == orig) filtered else vs
        manager.show(
          graph_operations.InducedEdgeBundle(),
          'input -> eb, 'srcSubset -> f(eb.srcVertexSet), 'dstSubset -> f(eb.dstVertexSet))
      }
      FEStatus.success
    }
  }

  registerOperation(ExampleGraph)
  object ExampleGraph extends FEOperation {
    val title = "Example Graph"
    val category = "Vertex operations"
    val parameters = Seq()
    def apply(params: Map[String, String]) = {
      manager.show(graph_operations.ExampleGraph())
      FEStatus.success
    }
  }

  registerOperation(AttributeConversion)
  object AttributeConversion extends FEOperation {
    val title = "Convert attributes"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("vattrs", "Vertex attributes", kind = "multi-vertex-attribute"),
      Param("eattrs", "Edge attributes", kind = "multi-edge-attribute"),
      Param("type", "Convert into", options = UIValue.seq("string", "double")))

    def apply(params: Map[String, String]): FEStatus = {
      val vattrs: Seq[String] = if (params("vattrs").isEmpty) Nil else params("vattrs").split(",")
      val eattrs: Seq[String] = if (params("eattrs").isEmpty) Nil else params("eattrs").split(",")
      val vas = vattrs.map(s => manager.vertexAttribute(s.asUUID))
      val eas = eattrs.map(s => manager.edgeAttribute(s.asUUID))
      val typ = params("type")
      if (typ == "string") {
        val okVAs = vas.filter(!_.is[String])
        val okEAs = eas.filter(!_.is[String])
        if (okVAs.isEmpty && okEAs.isEmpty) return FEStatus.failure("Nothing to convert.")
        for (va <- okVAs) manager.show(graph_operations.VertexAttributeToString(), 'attr -> va)
        for (ea <- okEAs) manager.show(graph_operations.EdgeAttributeToString(), 'attr -> ea)
      } else if (typ == "double") {
        val okVAs = vas.filter(_.is[String])
        val okEAs = eas.filter(_.is[String])
        if (okVAs.isEmpty && okEAs.isEmpty) return FEStatus.failure("Nothing to convert.")
        for (va <- okVAs) manager.show(graph_operations.VertexAttributeToDouble(), 'attr -> va)
        for (ea <- okEAs) manager.show(graph_operations.EdgeAttributeToDouble(), 'attr -> ea)
      } else assert(false, s"Unexpected type: $typ")
      FEStatus.success
    }
  }

  registerOperation(AddReversedEdges)
  object AddReversedEdges extends FEOperation {
    val title = "Add reversed edges"
    val category = "Edge operations"
    val parameters = Seq(
      Param("es", "Edges", kind = "edge-bundle"))
    def apply(params: Map[String, String]): FEStatus = {
      manager.show(graph_operations.AddReversedEdges(),
        'es -> manager.edgeBundle(params("es").asUUID))
      FEStatus.success
    }
  }

  registerOperation(EdgeGraph)
  object EdgeGraph extends FEOperation {
    val title = "Dual graph"
    val category = "Edge operations"
    val parameters = Seq(
      Param("es", "Edges", kind = "edge-bundle"))
    def apply(params: Map[String, String]): FEStatus = {
      manager.show(graph_operations.EdgeGraph(),
        'es -> manager.edgeBundle(params("es").asUUID))
      FEStatus.success
    }
  }

  registerOperation(PageRank)
  object PageRank extends FEOperation {
    val title = "PageRank"
    val category = "Attribute operations"
    val parameters = Seq(
      Param("ws", "Weights", kind = "edge-attribute"),
      Param("df", "Damping factor"),
      Param("iter", "Iterations"))
    def apply(params: Map[String, String]): FEStatus = {
      manager.show(graph_operations.PageRank(params("df").toDouble, params("iter").toInt),
        'weights -> manager.edgeAttribute(params("ws").asUUID))
      FEStatus.success
    }
  }

  registerOperation(RemoveNonSymmetricEdges)
  object RemoveNonSymmetricEdges extends FEOperation {
    val title = "Remove non-symmetric edges"
    val category = "Edge operations"
    val parameters = Seq(
      Param("es", "Edges", kind = "edge-bundle"))
    def apply(params: Map[String, String]): FEStatus = {
      manager.show(graph_operations.RemoveNonSymmetricEdges(),
        'es -> manager.edgeBundle(params("es").asUUID))
      FEStatus.success
    }
  }
}
