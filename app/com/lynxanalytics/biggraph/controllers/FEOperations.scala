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
    val parameters = Seq(
      Param("size", "Vertex set size"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.CreateVertexSet(params("size").toInt))
      FEStatus.success
    }
  }

  registerOperation(RandomEdgeBundle)
  object RandomEdgeBundle extends FEOperation {
    val title = "Create random edge bundle"
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

  registerOperation(ImportEdgeListWithNumericIDs)
  object ImportEdgeListWithNumericIDs extends FEOperation {
    val title = "Import edge list with numeric IDs "
    val parameters = Seq(
      Param("data", "Input data path (use * for wildcard)"),
      Param("awsId", "AWS Access Key ID (optional)"),
      Param("awsKey", "AWS Secret Access Key (optional)"),
      Param("header", """Header string (ie. "src","dst","age","gender")"""),
      Param("delimiter", "Delimiter to use for parsing", defaultValue = ","),
      Param("src", "Source ID field name (without quotation marks)"),
      Param("dst", "Destination ID field name (without quotation marks)"),
      Param("filter", "(optional) Filtering expression (use JavaScript syntax, equation must evaluate to True/False)"))
    def apply(params: Map[String, String]) = {
      manager.apply(
        graph_operations.ImportEdgeListWithNumericIDs(
          graph_operations.CSV(
            Filename(params("data"), params("awsId"), params("awsKey")),
            params("delimiter"),
            params("header"),
            graph_operations.Javascript(params("filter"))),
          params("src"),
          params("dst")))
      FEStatus.success
    }
  }

  registerOperation(ImportEdgeListWithStringIDs)
  object ImportEdgeListWithStringIDs extends FEOperation {
    val title = "Import edge list with string IDs "
    val parameters = Seq(
      Param("data", "Input data path (use * for wildcard)"),
      Param("awsId", "AWS Access Key ID (optional)"),
      Param("awsKey", "AWS Secret Access Key (optional)"),
      Param("header", """Header string (ie. "src","dst","age","gender")"""),
      Param("delimiter", "Delimiter to use for parsing", defaultValue = ","),
      Param("src", "Source ID field name (without quotation marks)"),
      Param("dst", "Destination ID field name (without quotation marks)"),
      Param("vAttr", "Vertex attribute field name (without quotation marks)"),
      Param("filter", "(optional) Filtering expression (use JavaScript syntax, equation must evaluate to True/False)"))
    def apply(params: Map[String, String]) = {
      manager.apply(
        graph_operations.ImportEdgeListWithStringIDs(
          graph_operations.CSV(
            Filename(params("data"), params("awsId"), params("awsKey")),
            params("delimiter"),
            params("header"),
            graph_operations.Javascript(params("filter"))),
          params("src"),
          params("dst"),
          params("vAttr")))
      FEStatus.success
    }
  }

  registerOperation(FindMaxCliques)
  object FindMaxCliques extends FEOperation {
    val title = "Maximal cliques"
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

  registerOperation(SetOverlap)
  object SetOverlap extends FEOperation {
    val title = "Set overlaps (complete, regular)"
    val parameters = Seq(
      Param("vs", "Input vertex set", kind = "vertex-set"),
      Param("sets", "Vertex set of grouped input vertices", kind = "vertex-set"),
      Param("links", "Edge bundle linking the input and its groups", kind = "edge-bundle"),
      Param("min", "Minimum overlap size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.SetOverlap(params("min").toInt),
        'vs -> manager.vertexSet(params("vs").asUUID),
        'sets -> manager.vertexSet(params("sets").asUUID),
        'links -> manager.edgeBundle(params("links").asUUID))
      FEStatus.success
    }
  }

  registerOperation(UniformOverlapForCC)
  object UniformOverlapForCC extends FEOperation {
    val title = "Set overlaps (for connected components, regular)"
    val parameters = Seq(
      Param("vs", "Input vertex set", kind = "vertex-set"),
      Param("sets", "Vertex set of grouped input vertices", kind = "vertex-set"),
      Param("links", "Edge bundle linking the input and its groups", kind = "edge-bundle"),
      Param("min", "Minimum overlap size", defaultValue = "3"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.UniformOverlapForCC(params("min").toInt),
        'vs -> manager.vertexSet(params("vs").asUUID),
        'sets -> manager.vertexSet(params("sets").asUUID),
        'links -> manager.edgeBundle(params("links").asUUID))
      FEStatus.success
    }
  }

  registerOperation(InfocomOverlapForCC)
  object InfocomOverlapForCC extends FEOperation {
    val title = "Set overlaps (for connected components, infocom)"
    val parameters = Seq(
      Param("vs", "Input vertex set", kind = "vertex-set"),
      Param("sets", "Vertex set of grouped input vertices", kind = "vertex-set"),
      Param("links", "Edge bundle linking the input and its groups", kind = "edge-bundle"),
      Param("thr", "Adjacency threshold of infocom overlap function", defaultValue = "0.6"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.InfocomOverlapForCC(params("thr").toDouble),
        'vs -> manager.vertexSet(params("vs").asUUID),
        'sets -> manager.vertexSet(params("sets").asUUID),
        'links -> manager.edgeBundle(params("links").asUUID))
      FEStatus.success
    }
  }

  registerOperation(ConnectedComponents)
  object ConnectedComponents extends FEOperation {
    val title = "Connected components"
    val parameters = Seq(
      Param("vs", "Vertex set", kind = "vertex-set"),
      Param("es", "Edge bundle", kind = "edge-bundle"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.ConnectedComponents(),
        'vs -> manager.vertexSet(params("vs").asUUID),
        'es -> manager.edgeBundle(params("es").asUUID))
      FEStatus.success
    }
  }

  registerOperation(ConcatenateBundles)
  object ConcatenateBundles extends FEOperation {
    val title = "Concatenate edge bundles, weighted"
    val parameters = Seq(
      Param("vsA", "Vertex set A", kind = "vertex-set"),
      Param("vsB", "Vertex set B", kind = "vertex-set"),
      Param("vsC", "Vertex set C", kind = "vertex-set"),
      Param("esAB", "Edge bundle A->B", kind = "edge-bundle"),
      Param("esBC", "Edge bundle B->C", kind = "edge-bundle"),
      Param("wAB", "Edge weight A->B", kind = "multi-edge-attribute"),
      Param("wBC", "Edge weight B->C", kind = "multi-edge-attribute"))
    def apply(params: Map[String, String]) = {
      manager.apply(graph_operations.ConcatenateBundles(),
        'vsA -> manager.vertexSet(params("vsA").asUUID),
        'vsB -> manager.vertexSet(params("vsB").asUUID),
        'vsC -> manager.vertexSet(params("vsC").asUUID),
        'edgesAB -> manager.edgeBundle(params("esAB").asUUID),
        'edgesBC -> manager.edgeBundle(params("esBC").asUUID),
        'weightsAB -> manager.edgeAttribute(params("wAB").asUUID),
        'weightsBC -> manager.edgeAttribute(params("wBC").asUUID))
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
    val title = "Export vertex attributes to CSV"
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
