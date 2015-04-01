package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import play.api.libs.json
import scala.reflect.runtime.universe.typeOf

class Operations(env: BigGraphEnvironment) extends OperationRepository(env) {
  val Param = FEOperationParameterMeta // Short alias.

  import Operation.Category
  import Operation.Context
  // Categories.
  abstract class VertexOperation(c: Context)
    extends Operation(c, Category("Vertex operations", "blue"))
  abstract class EdgeOperation(c: Context)
    extends Operation(c, Category("Edge operations", "orange"))
  abstract class AttributeOperation(c: Context)
    extends Operation(c, Category("Attribute operations", "yellow"))
  abstract class CreateSegmentationOperation(c: Context)
    extends Operation(c, Category("Create segmentation", "green"))
  abstract class UtilityOperation(c: Context)
    extends Operation(c, Category("Utility operations", "green", icon = "wrench", sortKey = "zz"))
  trait SegOp extends Operation {
    protected def seg = project.asSegmentation
    protected def parent = seg.parent
  }
  abstract class SegmentationUtilityOperation(c: Context)
    extends Operation(c, Category(
      "Segmentation utility operations",
      "green",
      visible = c.project.isSegmentation,
      icon = "wrench",
      sortKey = "zz")) with SegOp
  abstract class SegmentationOperation(c: Context)
    extends Operation(c, Category(
      "Segmentation operations",
      "yellow",
      visible = c.project.isSegmentation)) with SegOp
  abstract class SegmentationWorkflowOperation(c: Context)
    extends Operation(c, Category(
      "Workflows on segmentation",
      "magenta",
      visible = c.project.isSegmentation)) with SegOp

  register(new VertexOperation(_) {
    val title = "Discard vertices"
    val description = ""
    def parameters = List()
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      project.vertexSet = null
    }
  })

  register(new EdgeOperation(_) {
    val title = "Discard edges"
    val description = ""
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      project.edgeBundle = null
    }
  })

  register(new VertexOperation(_) {
    val title = "New vertex set"
    val description = "Creates a new vertex set with no edges and no attributes."
    def parameters = List(
      Param("size", "Vertex set size"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val result = graph_operations.CreateVertexSet(params("size").toLong)().result
      project.setVertexSet(result.vs, idAttr = "id")
      project.vertexAttributes("ordinal") = result.ordinal
    }
  })

  register(new EdgeOperation(_) {
    val title = "Create random edge bundle"
    val description =
      """Creates edges randomly, so that each vertex will have a degree uniformly
      chosen between 0 and 2 × the provided parameter."""
    def parameters = List(
      Param("degree", "Average degree", defaultValue = "10.0"),
      Param("seed", "Seed", defaultValue = "0"))
    def enabled = hasVertexSet && hasNoEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FastRandomEdgeBundle(
        params("seed").toInt, params("degree").toDouble)
      project.edgeBundle = op(op.vs, project.vertexSet).result.es
    }
  })

  register(new EdgeOperation(_) {
    val title = "Connect vertices on attribute"
    val description =
      """Creates edges between vertices that are equal in a chosen attribute. If the source
      attribute of A equals the destination attribute of B, an A&nbsp;&rarr;&nbsp;B edge will
      be generated.
      """
    def parameters = List(
      Param("fromAttr", "Source attribute", options = vertexAttributes),
      Param("toAttr", "Destination attribute", options = vertexAttributes))
    def enabled =
      (hasVertexSet && hasNoEdgeBundle
        && FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes."))
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
    def apply(params: Map[String, String]) = {
      val fromAttrName = params("fromAttr")
      val toAttrName = params("toAttr")
      val fromAttr = project.vertexAttributes(fromAttrName)
      val toAttr = project.vertexAttributes(toAttrName)
      assert(fromAttr.typeTag.tpe =:= toAttr.typeTag.tpe,
        s"$fromAttrName and $toAttrName are not of the same type.")
      applyAB(fromAttr, toAttr)
    }
  })

  trait RowReader {
    def sourceParameters: List[FEOperationParameterMeta]
    def source(params: Map[String, String]): graph_operations.RowInput
  }

  val csvImportHelpText =
    """ Wildcard (foo/*.csv) and glob (foo/{bar,baz}.csv) patterns are accepted. S3 paths must
      include the key name and secret key in the following format:
        <tt>s3n://key_name:secret_key@bucket/dir/file</tt>
      """

  trait CSVRowReader extends RowReader {
    def sourceParameters = List(
      Param("files", "Files", kind = "file"),
      Param("header", "Header", defaultValue = "<read first line>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("filter", "(optional) Filtering expression"))
    def source(params: Map[String, String]) = {
      val files = Filename(params("files"))
      val header = if (params("header") == "<read first line>")
        graph_operations.ImportUtil.header(files) else params("header")
      graph_operations.CSV(
        files,
        params("delimiter"),
        header,
        JavaScript(params("filter")))
    }
  }

  val jdbcHelpText = """
    The database name is the JDBC connection string without the <tt>jdbc:</tt> prefix.
    (For example <tt>mysql://127.0.0.1/?user=batman&password=alfred</tt>.)"""
  val sqlImportHelpText = jdbcHelpText + """
    An integer column must be specified as the key, and you have to select a key range."""

  trait SQLRowReader extends RowReader {
    def sourceParameters = List(
      Param("db", "Database"),
      Param("table", "Table or view"),
      Param("columns", "Columns (comma separated)"),
      Param("key", "Key column"))
    def source(params: Map[String, String]) = {
      val columns = params("columns").split(",", -1).map(_.trim)
      graph_operations.DBTable(
        params("db"),
        params("table"),
        (columns.toSet + params("key")).toSeq, // Always include "key".
        params("key"))
    }
  }

  abstract class ImportVerticesOperation(c: Context)
      extends VertexOperation(c) with RowReader {
    def parameters = sourceParameters ++ List(
      Param("id-attr", "ID attribute name", defaultValue = "id"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val imp = graph_operations.ImportVertexList(source(params))().result
      project.vertexSet = imp.vertices
      project.vertexAttributes = imp.attrs.mapValues(_.entity)
      val idAttr = params("id-attr")
      assert(
        !project.vertexAttributes.contains(idAttr),
        s"The input also contains a field called '$idAttr'. Please pick a different name.")
      project.vertexAttributes(idAttr) = idAsAttribute(project.vertexSet)
    }
  }
  register(new ImportVerticesOperation(_) with CSVRowReader {
    val title = "Import vertices from CSV files"
    val description =
      """Imports vertices (no edges) from a CSV file, or files.
      Each field in the CSV will be accessible as a vertex attribute.
      An extra vertex attribute is generated to hold the internal vertex ID.
      """ + csvImportHelpText
  })
  register(new ImportVerticesOperation(_) with SQLRowReader {
    val title = "Import vertices from a database"
    val description =
      """Imports vertices (no edges) from a SQL database.
      An extra vertex attribute is generated to hold the internal vertex ID.
      """ + sqlImportHelpText
  })

  abstract class ImportEdgesForExistingVerticesOperation(c: Context)
      extends VertexOperation(c) with RowReader {
    def parameters = sourceParameters ++ List(
      Param("attr", "Vertex id attribute", options = vertexAttributes[String]),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"))
    def enabled =
      hasNoEdgeBundle &&
        hasVertexSet &&
        FEStatus.assert(vertexAttributes[String].nonEmpty, "No vertex attributes to use as id.")
    def apply(params: Map[String, String]) = {
      val src = params("src")
      val dst = params("dst")
      val attr = project.vertexAttributes(params("attr")).runtimeSafeCast[String]
      val op = graph_operations.ImportEdgeListForExistingVertexSet(source(params), src, dst)
      val imp = op(op.srcVidAttr, attr)(op.dstVidAttr, attr).result
      project.edgeBundle = imp.edges
      project.edgeAttributes = imp.attrs.mapValues(_.entity)
    }
  }
  register(new ImportEdgesForExistingVerticesOperation(_) with CSVRowReader {
    val title = "Import edges for existing vertices from CSV files"
    val description =
      """Imports edges from a CSV file, or files. Your vertices must have a key attribute, by which
      the edges can be attached to them.""" + csvImportHelpText
  })
  register(new ImportEdgesForExistingVerticesOperation(_) with SQLRowReader {
    val title = "Import edges for existing vertices from a database"
    val description =
      """Imports edges from a SQL database. Your vertices must have a key attribute, by which
      the edges can be attached to them.""" + sqlImportHelpText
  })

  abstract class ImportVerticesAndEdgesOperation(c: Context)
      extends VertexOperation(c) with RowReader {
    def parameters = sourceParameters ++ List(
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val src = params("src")
      val dst = params("dst")
      val imp = graph_operations.ImportEdgeList(source(params), src, dst)().result
      project.setVertexSet(imp.vertices, idAttr = "id")
      project.vertexAttributes("stringID") = imp.stringID
      project.edgeBundle = imp.edges
      project.edgeAttributes = imp.attrs.mapValues(_.entity)
    }
  }
  register(new ImportVerticesAndEdgesOperation(_) with CSVRowReader {
    val title = "Import vertices and edges from single CSV fileset"
    val description =
      """Imports edges from a CSV file, or files.
      Each field in the CSV will be accessible as an edge attribute.
      Vertices will be generated for the endpoints of the edges.
      Two vertex attributes will be generated.
      "stringID" will contain the ID string that was used in the CSV.
      "id" will contain the internal vertex ID.
      """ + csvImportHelpText
  })
  register(new ImportVerticesAndEdgesOperation(_) with SQLRowReader {
    val title = "Import vertices and edges from single database table"
    val description =
      """Imports edges from a SQL database.
      Each column in the table will be accessible as an edge attribute.
      Vertices will be generated for the endpoints of the edges.
      Two vertex attributes will be generated.
      "stringID" will contain the ID string that was used in the database.
      "id" will contain the internal vertex ID.
      """ + sqlImportHelpText
  })

  register(new EdgeOperation(_) {
    val title = "Convert vertices into edges"
    val description =
      """Re-interprets the vertices as edges. You select two string-typed vertex attributes
      which specify the source and destination of the edges. An example use-case is if your
      vertices are calls. The converted graph will have subscribers as its vertices and the
      calls as its edges."""
    def parameters = List(
      Param("src", "Source", options = vertexAttributes[String]),
      Param("dst", "Destination", options = vertexAttributes[String]))
    def enabled = hasNoEdgeBundle &&
      FEStatus.assert(vertexAttributes[String].size > 2, "Two string attributes are needed.")
    def apply(params: Map[String, String]) = {
      val srcAttr = project.vertexAttributes(params("src")).runtimeSafeCast[String]
      val dstAttr = project.vertexAttributes(params("dst")).runtimeSafeCast[String]
      val newGraph = {
        val op = graph_operations.VerticesToEdges()
        op(op.srcAttr, srcAttr)(op.dstAttr, dstAttr).result
      }
      val oldAttrs = project.vertexAttributes.toMap
      project.vertexSet = newGraph.vs
      project.edgeBundle = newGraph.es
      project.vertexAttributes("stringID") = newGraph.stringID
      for ((name, attr) <- oldAttrs) {
        project.edgeAttributes(name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(
            attr, newGraph.embedding)
      }
    }
  })

  abstract class ImportVertexAttributesOperation(c: Context)
      extends VertexOperation(c) with RowReader {
    def parameters = sourceParameters ++ List(
      Param("id-attr", "Vertex id attribute", options = vertexAttributes[String]),
      Param("id-field", "ID field in the CSV file"),
      Param("prefix", "Name prefix for the imported vertex attributes", defaultValue = ""))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val idAttr = project.vertexAttributes(params("id-attr")).runtimeSafeCast[String]
      val op = graph_operations.ImportAttributesForExistingVertexSet(source(params), params("id-field"))
      val res = op(op.idAttr, idAttr).result
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((name, attr) <- res.attrs) {
        project.vertexAttributes(prefix + name) = attr
      }
    }
  }
  register(new ImportVertexAttributesOperation(_) with CSVRowReader {
    val title = "Import vertex attributes from CSV files"
    val description =
      """Imports vertex attributes for existing vertices from a CSV file.
      """ + csvImportHelpText
  })
  register(new ImportVertexAttributesOperation(_) with SQLRowReader {
    val title = "Import vertex attributes from a database"
    val description =
      """Imports vertex attributes for existing vertices from a SQL database.
      """ + sqlImportHelpText
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Maximal cliques"
    val description = ""
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "maximal_cliques"),
      Param("bothdir", "Edges required in both directions", options = UIValue.list(List("true", "false"))),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FindMaxCliques(params("min").toInt, params("bothdir").toBoolean)
      val result = op(op.es, project.edgeBundle).result
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(result.segments, idAttr = "id")
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
    }
  })

  register(new SegmentationUtilityOperation(_) {
    val title = "Check cliques"
    val description = "Validates that the given segmentations are in fact cliques."
    def parameters = List(
      Param("selected", "Clique ids to check", defaultValue = "<All>"),
      Param("bothdir", "Edges required in both directions", options = UIValue.list(List("true", "false"))))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val selected =
        if (params("selected") == "<All>") None
        else Some(params("selected").split(",", -1).map(_.toLong).toSet)
      val op = graph_operations.CheckClique(selected, params("bothdir").toBoolean)
      val result = op(op.es, parent.edgeBundle)(op.belongsTo, seg.belongsTo).result
      parent.scalars("invalid_cliques") = result.invalid
    }
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Connected components"
    val description = ""
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "connected_components"),
      Param(
        "type",
        "Connectedness type",
        options = UIValue.list(List("weak", "strong"))))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val symmetric = params("type") match {
        case "weak" => addReversed(project.edgeBundle)
        case "strong" => removeNonSymmetric(project.edgeBundle)
      }
      val op = graph_operations.ConnectedComponents()
      val result = op(op.es, symmetric).result
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(result.segments, idAttr = "id")
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
    }
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Find infocom communities"
    val description = ""
    def parameters = List(
      Param(
        "cliques_name", "Name for maximal cliques segmentation", defaultValue = "maximal_cliques"),
      Param(
        "communities_name", "Name for communities segmentation", defaultValue = "communities"),
      Param("bothdir", "Edges required in cliques in both directions", options = UIValue.list(List("true", "false"))),
      Param("min_cliques", "Minimum clique size", defaultValue = "3"),
      Param("adjacency_threshold", "Adjacency threshold for clique overlaps", defaultValue = "0.6"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val cliquesResult = {
        val op = graph_operations.FindMaxCliques(
          params("min_cliques").toInt, params("bothdir").toBoolean)
        op(op.es, project.edgeBundle).result
      }

      val cliquesSegmentation = project.segmentation(params("cliques_name"))
      cliquesSegmentation.project.setVertexSet(cliquesResult.segments, idAttr = "id")
      cliquesSegmentation.project.notes = "Maximal cliques of %s".format(project.projectName)
      cliquesSegmentation.belongsTo = cliquesResult.belongsTo
      cliquesSegmentation.project.vertexAttributes("size") =
        computeSegmentSizes(cliquesSegmentation)

      val cedges = {
        val op = graph_operations.InfocomOverlapForCC(params("adjacency_threshold").toDouble)
        op(op.belongsTo, cliquesResult.belongsTo).result.overlaps
      }

      val ccResult = {
        val op = graph_operations.ConnectedComponents()
        op(op.es, cedges).result
      }

      val weightedVertexToClique = const(cliquesResult.belongsTo)
      val weightedCliqueToCommunity = const(ccResult.belongsTo)

      val vertexToCommunity = {
        val op = graph_operations.ConcatenateBundles()
        op(
          op.edgesAB, cliquesResult.belongsTo)(
            op.edgesBC, ccResult.belongsTo)(
              op.weightsAB, weightedVertexToClique)(
                op.weightsBC, weightedCliqueToCommunity).result.edgesAC
      }

      val communitiesSegmentation = project.segmentation(params("communities_name"))
      communitiesSegmentation.project.setVertexSet(ccResult.segments, idAttr = "id")
      communitiesSegmentation.project.notes =
        "Infocom Communities of %s".format(project.projectName)
      communitiesSegmentation.belongsTo = vertexToCommunity
      communitiesSegmentation.project.vertexAttributes("size") =
        computeSegmentSizes(communitiesSegmentation)
    }
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Modular clustering"
    val description = "Tries to find a clustering of the graph with high modularity."
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "modular_clusters"),
      Param("weights", "Weight attribute", options =
        UIValue("", "no weight") +: edgeAttributes[Double]))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val edgeBundle = project.edgeBundle
      val weightsName = params("weights")
      val weights =
        if (weightsName == "") const(edgeBundle)
        else project.edgeAttributes(weightsName).runtimeSafeCast[Double]
      val result = {
        val op = graph_operations.FindModularClusteringByTweaks()
        op(op.edges, edgeBundle)(op.weights, weights).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(result.clusters, idAttr = "id")
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
      segmentation.project.vertexAttributes("size") =
        computeSegmentSizes(segmentation)

      val symmetricDirection = Direction("all edges", project.edgeBundle)
      val symmetricEdges = symmetricDirection.edgeBundle
      val symmetricWeights = symmetricDirection.pull(weights)
      val modularity = {
        val op = graph_operations.Modularity()
        op(op.edges, symmetricEdges)(op.weights, symmetricWeights)(op.belongsTo, result.belongsTo)
          .result.modularity
      }
      segmentation.project.scalars("modularity") = modularity
    }
  })

  register(new AttributeOperation(_) {
    val title = "Internal vertex ID as attribute"
    val description =
      """Exposes the internal vertex ID as an attribute. This attribute is automatically generated
      by operations that generate new vertex sets. But you can regenerate it with this operation
      if necessary."""
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "id"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      project.vertexAttributes(params("name")) = idAsAttribute(project.vertexSet)
    }
  })

  def idAsAttribute(vs: VertexSet) = {
    graph_operations.IdAsAttribute.run(vs)
  }

  register(new AttributeOperation(_) {
    val title = "Add gaussian vertex attribute"
    val description =
      "Generates a new random double attribute with a Gaussian distribution."
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "random"),
      Param("seed", "Seed", defaultValue = "0"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.AddGaussianVertexAttribute(params("seed").toInt)
      project.vertexAttributes(params("name")) = op(op.vertices, project.vertexSet).result.attr
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add constant edge attribute"
    val description = ""
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"),
      Param("type", "Type", options = UIValue.list(List("Double", "String"))))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val res = {
        if (params("type") == "Double") {
          const(project.edgeBundle, params("value").toDouble)
        } else {
          graph_operations.AddConstantAttribute.run(project.edgeBundle.idSet, params("value"))
        }
      }
      project.edgeAttributes(params("name")) = res
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add constant vertex attribute"
    val description = ""
    def parameters = List(
      Param("name", "Attribute name"),
      Param("value", "Value", defaultValue = "1"),
      Param("type", "Type", options = UIValue.list(List("Double", "String"))))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op: graph_operations.AddConstantAttribute[_] =
        graph_operations.AddConstantAttribute.doubleOrString(
          isDouble = (params("type") == "Double"), params("value"))
      project.vertexAttributes(params("name")) = op(op.vs, project.vertexSet).result.attr
    }
  })

  register(new AttributeOperation(_) {
    val title = "Fill with constant default value"
    val description =
      """An attribute may not be defined on every vertex. This operation sets a default value
      for the vertices where it was not defined."""
    def parameters = List(
      Param("attr", "Vertex attribute", options = vertexAttributes[String] ++ vertexAttributes[Double]),
      Param("def", "Default value"))
    def enabled = FEStatus.assert(
      (vertexAttributes[String] ++ vertexAttributes[Double]).nonEmpty, "No vertex attributes.")
    def apply(params: Map[String, String]) = {
      val attr = project.vertexAttributes(params("attr"))
      val op: graph_operations.AddConstantAttribute[_] =
        graph_operations.AddConstantAttribute.doubleOrString(
          isDouble = attr.is[Double], params("def"))
      val default = op(op.vs, project.vertexSet).result
      project.vertexAttributes(params("attr")) = unifyAttribute(attr, default.attr.entity)
    }
  })

  register(new AttributeOperation(_) {
    val title = "Merge two attributes"
    val description =
      """An attribute may not be defined on every vertex. This operation uses the secondary
      attribute to fill in the values where the primary attribute is undefined. If both are
      undefined on a vertex then the result is undefined too."""
    def parameters = List(
      Param("name", "New attribute name", defaultValue = ""),
      Param("attr1", "Primary attribute", options = vertexAttributes),
      Param("attr2", "Secondary attribute", options = vertexAttributes))
    def enabled = FEStatus.assert(
      vertexAttributes.size >= 2, "Not enough vertex attributes.")
    def apply(params: Map[String, String]) = {
      val name = params("name")
      assert(name.nonEmpty, "You must specify a name for the new attribute.")
      val attr1 = project.vertexAttributes(params("attr1"))
      val attr2 = project.vertexAttributes(params("attr2"))
      assert(attr1.typeTag.tpe =:= attr2.typeTag.tpe,
        "The two attributes must have the same type.")
      project.vertexAttributes(name) = unifyAttribute(attr1, attr2)
    }
  })

  register(new EdgeOperation(_) {
    val title = "Reverse edge direction"
    val description = ""
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, project.edgeBundle).result
      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        res.esBA,
        res.injection)
    }
  })

  register(new EdgeOperation(_) {
    val title = "Add reversed edges"
    val description =
      """Using this operation you end up with a graph with symmetric edges: if there is an
      edge from A->B then there is a corresponding edge from B->A. This is the closest you
      can get to an "undirected" graph."""
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val dir = Direction("all edges", project.edgeBundle)
      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        dir.edgeBundle,
        dir.pullBundleOpt.get)
    }
  })

  register(new AttributeOperation(_) {
    val title = "Clustering coefficient"
    val description = ""
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "clustering_coefficient"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.ClusteringCoefficient()
      project.vertexAttributes(params("name")) = op(op.es, project.edgeBundle).result.clustering
    }
  })

  register(new AttributeOperation(_) {
    val title = "Embeddedness"
    val description = "Calculates the overlap size of vertex neighborhoods along the edges."
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "embeddedness"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.Embeddedness()
      project.edgeAttributes(params("name")) = op(op.es, project.edgeBundle).result.embeddedness
    }
  })

  register(new AttributeOperation(_) {
    val title = "Dispersion"
    val description = """Calculates in what extent a given edge acts as intermediary between the
    the mutual neighbors of its vertices. Might be useful for locating romantic partnerships based
    on network structure in a social network."""
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "dispersion"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val dispersion = {
        val op = graph_operations.Dispersion()
        op(op.es, project.edgeBundle).result.dispersion.entity
      }
      val embeddedness = {
        val op = graph_operations.Embeddedness()
        op(op.es, project.edgeBundle).result.embeddedness.entity
      }
      // http://arxiv.org/pdf/1310.6753v1.pdf
      var normalizedDispersion = {
        val op = graph_operations.DeriveJSDouble(
          JavaScript("Math.pow(disp, 0.61) / (emb + 5)"),
          Seq("disp", "emb"))
        op(op.attrs, graph_operations.VertexAttributeToJSValue.seq(
          dispersion, embeddedness)).result.attr.entity
      }
      // TODO: recursive dispersion
      project.edgeAttributes(params("name")) = dispersion
      project.edgeAttributes("normalized_" + params("name")) = normalizedDispersion
    }
  })

  register(new AttributeOperation(_) {
    val title = "Degree"
    val description = ""
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "degree"),
      Param("direction", "Count", options = Direction.options))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val es = Direction(params("direction"), project.edgeBundle, reversed = true).edgeBundle
      val op = graph_operations.OutDegree()
      project.vertexAttributes(params("name")) = op(op.es, es).result.outDegree
    }
  })

  register(new AttributeOperation(_) {
    val title = "PageRank"
    val description = ""
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "page_rank"),
      Param("weights", "Weight attribute", options = edgeAttributes[Double]),
      Param("iterations", "Number of iterations", defaultValue = "5"),
      Param("damping", "Damping factor", defaultValue = "0.85"))
    def enabled = FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes.")
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.PageRank(params("damping").toDouble, params("iterations").toInt)
      val weights = project.edgeAttributes(params("weights")).runtimeSafeCast[Double]
      project.vertexAttributes(params("name")) =
        op(op.es, project.edgeBundle)(op.weights, weights).result.pagerank
    }
  })

  register(new VertexOperation(_) {
    val title = "Example Graph"
    val description =
      "Creates small test graph with 4 people and 4 edges between them."
    def parameters = List()
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val g = graph_operations.ExampleGraph()().result
      project.vertexSet = g.vertices
      project.edgeBundle = g.edges
      project.vertexAttributes = g.vertexAttributes.mapValues(_.entity)
      project.vertexAttributes("id") = idAsAttribute(project.vertexSet)
      project.edgeAttributes = g.edgeAttributes.mapValues(_.entity)
    }
  })

  private val toStringHelpText = "Converts the selected %s attributes to string type."
  register(new AttributeOperation(_) {
    val title = "Vertex attribute to string"
    val description = toStringHelpText.format("vertex")
    def parameters = List(
      Param("attr", "Vertex attribute", options = vertexAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
    def apply(params: Map[String, String]) = {
      for (attr <- params("attr").split(",", -1)) {
        project.vertexAttributes(attr) = attributeToString(project.vertexAttributes(attr))
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Edge attribute to string"
    val description = toStringHelpText.format("edge")
    def parameters = List(
      Param("attr", "Edge attribute", options = edgeAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
    def apply(params: Map[String, String]) = {
      for (attr <- params("attr").split(",", -1)) {
        project.edgeAttributes(attr) = attributeToString(project.edgeAttributes(attr))
      }
    }
  })

  private val toDoubleHelpText =
    """Converts the selected string typed %s attributes to double (double precision floating point
    number) type.
    """
  register(new AttributeOperation(_) {
    val title = "Vertex attribute to double"
    val description = toDoubleHelpText.format("vertex")
    val eligible = vertexAttributes[String] ++ vertexAttributes[Long]
    def parameters = List(
      Param("attr", "Vertex attribute", options = eligible, multipleChoice = true))
    def enabled = FEStatus.assert(eligible.nonEmpty, "No eligible vertex attributes.")
    def apply(params: Map[String, String]) = {
      for (name <- params("attr").split(",", -1)) {
        val attr = project.vertexAttributes(name)
        project.vertexAttributes(name) = toDouble(attr)
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Edge attribute to double"
    val description = toDoubleHelpText.format("edge")
    val eligible = edgeAttributes[String] ++ edgeAttributes[Long]
    def parameters = List(
      Param("attr", "Edge attribute", options = eligible, multipleChoice = true))
    def enabled = FEStatus.assert(eligible.nonEmpty, "No eligible edge attributes.")
    def apply(params: Map[String, String]) = {
      for (name <- params("attr").split(",", -1)) {
        val attr = project.edgeAttributes(name)
        project.edgeAttributes(name) = toDouble(attr)
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Vertex attributes to position"
    val description =
      """Creates an attribute of type <tt>(Double, Double)</tt> from two <tt>Double</tt> attributes.
      The created attribute can be used as an X-Y or latitude-longitude location."""
    def parameters = List(
      Param("output", "Save as", defaultValue = "position"),
      Param("x", "X or latitude", options = vertexAttributes[Double]),
      Param("y", "Y or longitude", options = vertexAttributes[Double]))
    def enabled = FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes.")
    def apply(params: Map[String, String]) = {
      assert(params("output").nonEmpty, "Please set an attribute name.")
      val pos = {
        val op = graph_operations.JoinAttributes[Double, Double]()
        val x = project.vertexAttributes(params("x")).runtimeSafeCast[Double]
        val y = project.vertexAttributes(params("y")).runtimeSafeCast[Double]
        op(op.a, x)(op.b, y).result.attr
      }
      project.vertexAttributes(params("output")) = pos
    }
  })

  register(new VertexOperation(_) {
    val title = "Edge graph"
    val description =
      """Creates the edge graph (aka line graph), where each vertex corresponds to an edge in the
      current graph. The vertices will be connected, if one corresponding edge is the continuation
      of the other.
      """
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.EdgeGraph()
      val g = op(op.es, project.edgeBundle).result
      project.setVertexSet(g.newVS, idAttr = "id")
      project.edgeBundle = g.newES
    }
  })

  register(new AttributeOperation(_) {
    val title = "Derived vertex attribute"
    val description =
      """Generates a new attribute based on existing attributes. The value expression can be
      an arbitrary JavaScript expression, and it can refer to existing attributes as if they
      were local variables. For example you can write <tt>age * 2</tt> to generate a new attribute
      that is the double of the age attribute. Or you can write
      <tt>gender == 'Male' ? 'Mr ' + name : 'Ms ' + name</tt> for a more complex example.
      """
    def parameters = List(
      Param("output", "Save as"),
      Param("type", "Result type", options = UIValue.list(List("double", "string"))),
      Param("expr", "Value", defaultValue = "1"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("output").nonEmpty, "Please set an output attribute name.")
      val expr = params("expr")
      val namedAttributes = project.vertexAttributes
        .filter { case (name, attr) => expr.contains(name) }
        .toIndexedSeq
        .map { case (name, attr) => name -> graph_operations.VertexAttributeToJSValue.run(attr) }
      val js = JavaScript(expr)
      val op: graph_operations.DeriveJS[_] = params("type") match {
        case "string" =>
          graph_operations.DeriveJSString(js, namedAttributes.map(_._1))
        case "double" =>
          graph_operations.DeriveJSDouble(js, namedAttributes.map(_._1))
      }
      val result = op(op.attrs, namedAttributes.map(_._2)).result
      project.vertexAttributes(params("output")) = result.attr
    }
  })

  register(new AttributeOperation(_) {
    val title = "Derived edge attribute"
    val description =
      """Generates a new attribute based on existing attributes. The value expression can be
      an arbitrary JavaScript expression, and it can refer to existing attributes on the edge as if
      they were local variables. It can also refer to attributes of the source and destination
      vertex of the edge using the format src$attribute and dst$attribute.

      For example you can write <tt>weight * Math.abs(src$age - dst$age)</tt> to generate a new
      attribute that is the weighted age difference of the two endpoints of the edge.
      """
    def parameters = List(
      Param("output", "Save as"),
      Param("type", "Result type", options = UIValue.list(List("double", "string"))),
      Param("expr", "Value", defaultValue = "1"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val expr = params("expr")
      val edgeBundle = project.edgeBundle
      val namedEdgeAttributes = project.edgeAttributes
        .filter { case (name, attr) => expr.contains(name) }
        .toIndexedSeq
        .map { case (name, attr) => name -> graph_operations.VertexAttributeToJSValue.run(attr) }
      val namedSrcVertexAttributes = project.vertexAttributes
        .filter { case (name, attr) => expr.contains("src$" + name) }
        .toIndexedSeq
        .map {
          case (name, attr) =>
            val mappedAttr = graph_operations.VertexToEdgeAttribute.srcAttribute(attr, edgeBundle)
            "src$" + name -> graph_operations.VertexAttributeToJSValue.run(mappedAttr)
        }
      val namedDstVertexAttributes = project.vertexAttributes
        .filter { case (name, attr) => expr.contains("dst$" + name) }
        .toIndexedSeq
        .map {
          case (name, attr) =>
            val mappedAttr = graph_operations.VertexToEdgeAttribute.dstAttribute(attr, edgeBundle)
            "dst$" + name -> graph_operations.VertexAttributeToJSValue.run(mappedAttr)
        }

      val namedAttributes =
        namedEdgeAttributes ++ namedSrcVertexAttributes ++ namedDstVertexAttributes

      val js = JavaScript(expr)
      val op: graph_operations.DeriveJS[_] = params("type") match {
        case "string" =>
          graph_operations.DeriveJSString(js, namedAttributes.map(_._1))
        case "double" =>
          graph_operations.DeriveJSDouble(js, namedAttributes.map(_._1))
      }
      val result = op(op.attrs, namedAttributes.map(_._2)).result
      project.edgeAttributes(params("output")) = result.attr
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Aggregate to segmentation"
    val description = "For example, it can calculate the average age of each clique."
    def parameters = aggregateParams(parent.vertexAttributes)
    def enabled =
      FEStatus.assert(parent.vertexAttributes.nonEmpty,
        "No vertex attributes on parent")
    def apply(params: Map[String, String]) = {
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          seg.belongsTo,
          AttributeWithLocalAggregator(parent.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${attr}_${choice}") = result
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Weighted aggregate to segmentation"
    val description =
      "For example, it can calculate the average age per kilogram of each clique."
    def parameters = List(
      Param("weight", "Weight", options = vertexAttributes[Double])) ++
      aggregateParams(parent.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(parent.vertexAttributeNames[Double].nonEmpty,
        "No numeric vertex attributes on parent")
    def apply(params: Map[String, String]) = {
      val weightName = params("weight")
      val weight = parent.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          seg.belongsTo,
          AttributeWithWeightedAggregator(weight, parent.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Aggregate from segmentation"
    val description =
      "For example, it can calculate the average size of cliques a person belongs to."
    def parameters = List(
      Param("prefix", "Generated name prefix",
        defaultValue = project.asSegmentation.name)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          reverse(seg.belongsTo),
          AttributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        seg.parent.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Weighted aggregate from segmentation"
    val description =
      """For example, it can calculate an averge over the cliques a person belongs to,
      weighted by the size of the cliques."""
    def parameters = List(
      Param("prefix", "Generated name prefix",
        defaultValue = project.asSegmentation.name),
      Param("weight", "Weight", options = vertexAttributes[Double])) ++
      aggregateParams(project.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          reverse(seg.belongsTo),
          AttributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
        seg.parent.vertexAttributes(s"${prefix}${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Create edges from set overlaps"
    val description = "Connects segments with large enough overlaps."
    def parameters = List(
      Param("minOverlap", "Minimal overlap for connecting two segments", defaultValue = "3"))
    def enabled = hasNoEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.SetOverlap(params("minOverlap").toInt)
      val res = op(op.belongsTo, seg.belongsTo).result
      project.edgeBundle = res.overlaps
      project.edgeAttributes("Overlap size") =
        // Long is better supported on the frontend.
        graph_operations.IntAttributeToLong.run(res.overlapSize)
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Create edges from co-occurrence"
    val description =
      """Connects vertices in the parent project if they co-occur in any segments.
      Multiple co-occurrences will result in multiple parallel edges. Loop edges
      are generated for each segment that a vertex belongs to."""
    def parameters = List()
    def enabled = FEStatus.assert(parent.edgeBundle == null, "Parent graph has edges already.")
    def apply(params: Map[String, String]) = {
      val op = graph_operations.EdgesFromSegmentation()
      parent.edgeBundle = op(op.belongsTo, seg.belongsTo).result.es
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate on neighbors"
    val description =
      "For example it can calculate the average age of the friends of each person."
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Param("direction", "Aggregate on", options = Direction.options)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes") && hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = Direction(params("direction"), project.edgeBundle).edgeBundle
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          edges,
          AttributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate on neighbors"
    val description =
      "For example it can calculate the average age per kilogram of the friends of each person."
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Param("weight", "Weight", options = vertexAttributes[Double]),
      Param("direction", "Aggregate on", options = Direction.options)) ++
      aggregateParams(project.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes") && hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = Direction(params("direction"), project.edgeBundle).edgeBundle
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((name, choice) <- parseAggregateParams(params)) {
        val attr = project.vertexAttributes(name)
        val result = aggregateViaConnection(
          edges,
          AttributeWithWeightedAggregator(weight, attr, choice))
        project.vertexAttributes(s"${prefix}${name}_${choice}_by_${weightName}") = result
      }
    }
  })

  register(new VertexOperation(_) {
    val title = "Merge vertices by attribute"
    val description =
      """Merges each set of vertices that are equal by the chosen attribute. Aggregations
      can be specified for how to handle the rest of the attributes, which may be different
      among the merged vertices."""
    def parameters = List(
      Param("key", "Match by", options = vertexAttributes)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def merge[T](attr: Attribute[T]): graph_operations.MergeVertices.Output = {
      val op = graph_operations.MergeVertices[T]()
      op(op.attr, attr).result
    }
    def apply(params: Map[String, String]) = {
      val m = merge(project.vertexAttributes(params("key")))
      val oldVAttrs = project.vertexAttributes.toMap
      val oldEdges = project.edgeBundle
      val oldEAttrs = project.edgeAttributes.toMap
      project.setVertexSet(m.segments, idAttr = "id")
      // Always use most_common for the key attribute.
      val hack = "aggregate-" + params("key") -> "most_common"
      for ((attr, choice) <- parseAggregateParams(params + hack)) {
        val result = aggregateViaConnection(
          m.belongsTo,
          AttributeWithLocalAggregator(oldVAttrs(attr), choice))
        project.vertexAttributes(attr) = result
      }
      if (oldEdges != null) {
        val edgeInduction = {
          val op = graph_operations.InducedEdgeBundle()
          op(op.srcMapping, m.belongsTo)(op.dstMapping, m.belongsTo)(op.edges, oldEdges).result
        }
        project.edgeBundle = edgeInduction.induced
        for ((name, eAttr) <- oldEAttrs) {
          project.edgeAttributes(name) =
            graph_operations.PulledOverVertexAttribute.pullAttributeVia(
              eAttr, edgeInduction.embedding)
        }
      }
    }
  })

  register(new EdgeOperation(_) {
    val title = "Merge parallel edges"
    val description = ""

    def parameters =
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) })

    def enabled = hasEdgeBundle

    def apply(params: Map[String, String]) = {
      val edgesAsAttr = {
        val op = graph_operations.EdgeBundleAsAttribute()
        op(op.edges, project.edgeBundle).result.attr
      }
      val mergedResult = {
        val op = graph_operations.MergeVertices[(ID, ID)]()
        op(op.attr, edgesAsAttr).result
      }
      val newEdges = {
        val op = graph_operations.PulledOverEdges()
        op(op.originalEB, project.edgeBundle)(op.injection, mergedResult.representative)
          .result.pulledEB
      }
      val oldAttrs = project.edgeAttributes.toMap
      project.edgeBundle = newEdges

      for ((attrName, choice) <- parseAggregateParams(params)) {
        project.edgeAttributes(attrName) =
          aggregateViaConnection(
            mergedResult.belongsTo,
            AttributeWithLocalAggregator(oldAttrs(attrName), choice))
      }
    }
  })

  register(new EdgeOperation(_) {
    val title = "Discard loop edges"
    val description = "Discards edges that connect a vertex to itself."
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val edgesAsAttr = {
        val op = graph_operations.EdgeBundleAsAttribute()
        op(op.edges, project.edgeBundle).result.attr
      }
      val guid = edgesAsAttr.entity.gUID.toString
      val embedding = FEFilters.embedFilteredVertices(
        project.edgeBundle.idSet,
        Seq(FEVertexAttributeFilter(guid, "!=")))
      project.pullBackEdges(embedding)
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate vertex attribute globally"
    val description = "The result is a single scalar value."
    def parameters = List(Param("prefix", "Generated name prefix", defaultValue = "")) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(AttributeWithAggregator(project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate vertex attribute globally"
    val description = "The result is a single scalar value."
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = ""),
      Param("weight", "Weight", options = vertexAttributes[Double])) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}_by_${weightName}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate edge attribute globally"
    val description = "The result is a single scalar value."
    def parameters = List(Param("prefix", "Generated name prefix", defaultValue = "")) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) },
        needsGlobal = true)
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithAggregator(project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate edge attribute globally"
    val description = "The result is a single scalar value."
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = ""),
      Param("weight", "Weight", options = edgeAttributes[Double])) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) },
        needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithWeightedAggregator(weight, project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}_by_${weightName}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate edge attribute to vertices"
    val description =
      "For example it can calculate the average duration of calls for each person."
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Param("direction", "Aggregate on", options = Direction.attrOptions)) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) })
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      val direction = Direction(params("direction"), project.edgeBundle)
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          direction.edgeBundle,
          AttributeWithLocalAggregator(
            direction.pull(project.edgeAttributes(attr)),
            choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate edge attribute to vertices"
    val description =
      "For example it can calculate the average cost per second of calls for each person."
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Param("weight", "Weight", options = edgeAttributes[Double]),
      Param("direction", "Aggregate on", options = Direction.attrOptions)) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) },
        weighted = true)
    def enabled =
      FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes")
    def apply(params: Map[String, String]) = {
      val direction = Direction(params("direction"), project.edgeBundle)
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          direction.edgeBundle,
          AttributeWithWeightedAggregator(
            direction.pull(weight),
            direction.pull(project.edgeAttributes(attr)),
            choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register(new UtilityOperation(_) {
    val title = "No operation"
    val description = "Placeholder when creating new operations."
    def parameters = List()
    def enabled = FEStatus.enabled
    def apply(params: Map[String, String]) = {}
  })

  register(new UtilityOperation(_) {
    val title = "Discard edge attribute"
    val description = ""
    def parameters = List(
      Param("name", "Name", options = edgeAttributes))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      project.edgeAttributes(params("name")) = null
    }
  })

  register(new UtilityOperation(_) {
    val title = "Discard vertex attribute"
    val description = ""
    def parameters = List(
      Param("name", "Name", options = vertexAttributes))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      project.vertexAttributes(params("name")) = null
    }
  })

  register(new UtilityOperation(_) {
    val title = "Discard segmentation"
    val description = ""
    def parameters = List(
      Param("name", "Name", options = segmentations))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]) = {
      project.segmentation(params("name")).remove
    }
  })

  register(new UtilityOperation(_) {
    val title = "Discard scalar"
    val description = ""
    def parameters = List(
      Param("name", "Name", options = scalars))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    def apply(params: Map[String, String]) = {
      project.scalars(params("name")) = null
    }
  })

  register(new UtilityOperation(_) {
    val title = "Rename edge attribute"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = edgeAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      assert(!project.edgeAttributes.contains(params("to")),
        s"""An edge-attribute named '${params("to")}' already exists,
            please discard it or choose another name""")
      project.edgeAttributes(params("to")) = project.edgeAttributes(params("from"))
      project.edgeAttributes(params("from")) = null
    }
  })

  register(new UtilityOperation(_) {
    val title = "Rename vertex attribute"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = vertexAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      assert(!project.vertexAttributes.contains(params("to")),
        s"""A vertex-attribute named '${params("to")}' already exists,
            please discard it or choose another name""")
      assert(params("to").nonEmpty, "Please set the new attribute name.")
      project.vertexAttributes(params("to")) = project.vertexAttributes(params("from"))
      project.vertexAttributes(params("from")) = null
    }
  })

  register(new UtilityOperation(_) {
    val title = "Rename segmentation"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = segmentations),
      Param("to", "New name"))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]) = {
      assert(!project.segmentations.contains(params("to")),
        s"""A segmentation named '${params("to")}' already exists,
            please discard it or choose another name""")
      project.segmentation(params("from")).rename(params("to"))
    }
  })

  register(new UtilityOperation(_) {
    val title = "Rename scalar"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = scalars),
      Param("to", "New name"))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    def apply(params: Map[String, String]) = {
      assert(!project.scalars.contains(params("to")),
        s"""A scalar named '${params("to")}' already exists,
            please discard it or choose another name""")
      project.scalars(params("to")) = project.scalars(params("from"))
      project.scalars(params("from")) = null
    }
  })

  register(new UtilityOperation(_) {
    val title = "Copy edge attribute"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = edgeAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      project.edgeAttributes(params("to")) = project.edgeAttributes(params("from"))
    }
  })

  register(new UtilityOperation(_) {
    val title = "Copy vertex attribute"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = vertexAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      assert(params("to").nonEmpty, "Please set the new attribute name.")
      project.vertexAttributes(params("to")) = project.vertexAttributes(params("from"))
    }
  })

  register(new UtilityOperation(_) {
    val title = "Copy segmentation"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = segmentations),
      Param("to", "New name"))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]) = {
      val from = project.segmentation(params("from"))
      val to = project.segmentation(params("to"))
      from.project.copy(to.project)
      to.belongsTo = from.belongsTo
    }
  })

  register(new UtilityOperation(_) {
    val title = "Copy scalar"
    val description = ""
    def parameters = List(
      Param("from", "Old name", options = scalars),
      Param("to", "New name"))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    def apply(params: Map[String, String]) = {
      project.scalars(params("to")) = project.scalars(params("from"))
    }
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Import project as segmentation"
    val description =
      """Copies another project into a new segmentation for this one. There will be no
      connections between the segments and the base vertices. You can import/create those via
      a new operation."""
    def parameters = List(
      Param("them", "Other project's name", options = otherProjects))
    private def otherProjects = readableProjects.filter(_.id != project.projectName)
    def enabled =
      hasVertexSet &&
        FEStatus.assert(otherProjects.size > 0, "This is the only project")
    def apply(params: Map[String, String]) = {
      val themName = params("them")
      assert(otherProjects.map(_.id).contains(themName), s"Unknown project: $themName")
      val them = Project(themName)
      assert(them.vertexSet != null, s"No vertex set in $them")
      val segmentation = project.segmentation(params("them"))
      them.copy(segmentation.project)
      val op = graph_operations.EmptyEdgeBundle()
      segmentation.belongsTo = op(op.src, project.vertexSet)(op.dst, them.vertexSet).result.eb
      segmentation.project.discardCheckpoints()
    }
  })

  abstract class LoadSegmentationLinksOperation(c: Context)
      extends SegmentationOperation(c) with RowReader {
    def parameters = sourceParameters ++ List(
      Param(
        "base-id-attr",
        s"Identifying vertex attribute in $parent",
        options = UIValue.list(parent.vertexAttributeNames[String].toList)),
      Param("base-id-field", s"Identifying field for $parent"),
      Param(
        "seg-id-attr",
        s"Identifying vertex attribute in $project",
        options = vertexAttributes[String]),
      Param("seg-id-field", s"Identifying field for $project"))
    def enabled =
      FEStatus.assert(
        vertexAttributes[String].nonEmpty, "No string vertex attributes in this segmentation") &&
        FEStatus.assert(
          parent.vertexAttributeNames[String].nonEmpty, "No string vertex attributes in parent")
    def apply(params: Map[String, String]) = {
      val baseIdAttr = parent.vertexAttributes(params("base-id-attr")).runtimeSafeCast[String]
      val segIdAttr = project.vertexAttributes(params("seg-id-attr")).runtimeSafeCast[String]
      val op = graph_operations.ImportEdgeListForExistingVertexSet(
        source(params), params("base-id-field"), params("seg-id-field"))
      seg.belongsTo = op(op.srcVidAttr, baseIdAttr)(op.dstVidAttr, segIdAttr).result.edges
    }
  }
  register(new LoadSegmentationLinksOperation(_) with CSVRowReader {
    val title = "Load segmentation links from CSV"
    val description =
      "Import the connection between the main project and this segmentation from a CSV." +
        csvImportHelpText
  })
  register(new LoadSegmentationLinksOperation(_) with SQLRowReader {
    val title = "Load segmentation links from a database"
    val description =
      "Import the connection between the main project and this segmentation from a SQL database." +
        sqlImportHelpText
  })

  register(new SegmentationOperation(_) {
    val title = "Define segmentation links from matching attributes"
    val description =
      "Connect vertices in the main project with segmentations based on matching attributes."
    def parameters = List(
      Param(
        "base-id-attr",
        s"Identifying vertex attribute in $parent",
        options = UIValue.list(parent.vertexAttributeNames[String].toList)),
      Param(
        "seg-id-attr",
        s"Identifying vertex attribute in $project",
        options = vertexAttributes[String]))
    def enabled =
      FEStatus.assert(
        vertexAttributes[String].nonEmpty, "No string vertex attributes in this segmentation") &&
        FEStatus.assert(
          parent.vertexAttributeNames[String].nonEmpty, "No string vertex attributes in parent")
    def apply(params: Map[String, String]) = {
      val baseIdAttr = parent.vertexAttributes(params("base-id-attr")).runtimeSafeCast[String]
      val segIdAttr = project.vertexAttributes(params("seg-id-attr")).runtimeSafeCast[String]
      val op = graph_operations.EdgesFromBipartiteAttributeMatches[String]()
      seg.belongsTo = op(op.fromAttr, baseIdAttr)(op.toAttr, segIdAttr).result.edges
    }
  })

  register(new VertexOperation(_) {
    val title = "Union with another project"
    val description =
      """The resulting graph is just a disconnected graph containing the vertices and edges of
      the two originating projects. All vertex and edge attributes are preserved. If an attribute
      exists in both projects, it must have the same data type in both.
      """
    def parameters = List(
      Param("other", "Other project's name", options = readableProjects),
      Param("id-attr", "ID attribute name", defaultValue = "new_id"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]): Unit = {
      val otherName = params("other")
      assert(readableProjects.map(_.id).contains(otherName), s"Unknown project: $otherName")
      val other = Project(otherName)
      if (other.vertexSet == null) {
        // Nothing to do
        return
      }
      val vsUnion = {
        val op = graph_operations.VertexSetUnion(2)
        op(op.vss, Seq(project.vertexSet, other.vertexSet)).result
      }
      val newVertexAttributes = unifyAttributes(
        project.vertexAttributes
          .map {
            case (name, attr) =>
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                reverse(vsUnion.injections(0)))
          },
        other.vertexAttributes
          .map {
            case (name, attr) =>
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                reverse(vsUnion.injections(1)))
          })
      val ebInduced = Option(project.edgeBundle).map { eb =>
        val op = graph_operations.InducedEdgeBundle()
        val mapping = vsUnion.injections(0)
        op(op.srcMapping, mapping)(op.dstMapping, mapping)(op.edges, project.edgeBundle).result
      }
      val otherEbInduced = Option(other.edgeBundle).map { eb =>
        val op = graph_operations.InducedEdgeBundle()
        val mapping = vsUnion.injections(1)
        op(op.srcMapping, mapping)(op.dstMapping, mapping)(op.edges, other.edgeBundle).result
      }

      val (newEdgeBundle, myEbInjection, otherEbInjection): (EdgeBundle, EdgeBundle, EdgeBundle) =
        if (ebInduced.isDefined && !otherEbInduced.isDefined) {
          (ebInduced.get.induced, ebInduced.get.embedding, null)
        } else if (!ebInduced.isDefined && otherEbInduced.isDefined) {
          (otherEbInduced.get.induced, null, otherEbInduced.get.embedding)
        } else if (ebInduced.isDefined && otherEbInduced.isDefined) {
          val idUnion = {
            val op = graph_operations.VertexSetUnion(2)
            op(
              op.vss,
              Seq(ebInduced.get.induced.idSet, otherEbInduced.get.induced.idSet))
              .result
          }
          val ebUnion = {
            val op = graph_operations.EdgeBundleUnion(2)
            op(
              op.ebs, Seq(ebInduced.get.induced.entity, otherEbInduced.get.induced.entity))(
                op.injections, idUnion.injections.map(_.entity)).result.union
          }
          (ebUnion,
            concat(reverse(idUnion.injections(0).entity), ebInduced.get.embedding),
            concat(reverse(idUnion.injections(1).entity), otherEbInduced.get.embedding))
        } else {
          (null, null, null)
        }
      val newEdgeAttributes = unifyAttributes(
        project.edgeAttributes
          .map {
            case (name, attr) => {
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                myEbInjection)
            }
          },
        other.edgeAttributes
          .map {
            case (name, attr) =>
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                otherEbInjection)
          })

      project.vertexSet = vsUnion.union
      project.vertexAttributes = newVertexAttributes
      val idAttr = params("id-attr")
      assert(
        !project.vertexAttributes.contains(idAttr),
        s"The project already contains a field called '$idAttr'. Please pick a different name.")
      project.vertexAttributes(idAttr) = idAsAttribute(project.vertexSet)
      project.edgeBundle = newEdgeBundle
      project.edgeAttributes = newEdgeAttributes
    }
  })

  register(new VertexOperation(_) {
    val title = "Fingerprinting based on attributes"
    val description =
      """In a graph that has two different string identifier attributes (e.g. Facebook ID and
      MSISDN) this operation will match the vertices that only have the first attribute defined
      with the vertices that only have the second attribute defined. For the well-matched vertices
      the new attributes will be added. (For example if a vertex only had an MSISDN and we found a
      matching Facebook ID, this will be saved as the Facebook ID of the vertex.)

      <p>The matched vertices will not be automatically merged, but this can easily be performed
      with the "Merge vertices by attribute" operation on either of the two identifier attributes.
      """
    def parameters = List(
      Param("leftName", "First ID attribute", options = vertexAttributes[String]),
      Param("rightName", "Second ID attribute", options = vertexAttributes[String]),
      Param("weights", "Edge weights",
        options = UIValue("no weights", "no weights") +: edgeAttributes[Double]),
      Param("mrew", "Minimum relative edge weight", defaultValue = "0.0"),
      Param("mo", "Minimum overlap", defaultValue = "1"),
      Param("ms", "Minimum similarity", defaultValue = "0.5"))
    def enabled =
      hasEdgeBundle &&
        FEStatus.assert(vertexAttributes[String].size >= 2, "Two string attributes are needed.")
    def apply(params: Map[String, String]): Unit = {
      val mrew = params("mrew").toDouble
      val mo = params("mo").toInt
      val ms = params("ms").toDouble
      assert(mo >= 1, "Minimum overlap cannot be less than 1.")
      val leftName = project.vertexAttributes(params("leftName")).runtimeSafeCast[String]
      val rightName = project.vertexAttributes(params("rightName")).runtimeSafeCast[String]
      val weights = if (params("weights") == "no weights") {
        const(project.edgeBundle)
      } else {
        project.edgeAttributes(params("weights")).runtimeSafeCast[Double]
      }

      // TODO: Calculate relative edge weight, filter the edge bundle and pull over the weights.
      assert(mrew == 0, "Minimum relative edge weight is not implemented yet.")

      val candidates = {
        val op = graph_operations.FingerprintingCandidates()
        op(op.es, project.edgeBundle)(op.leftName, leftName)(op.rightName, rightName)
          .result.candidates
      }
      val fingerprinting = {
        val op = graph_operations.Fingerprinting(mo, ms)
        op(
          op.leftEdges, project.edgeBundle)(
            op.leftEdgeWeights, weights)(
              op.rightEdges, project.edgeBundle)(
                op.rightEdgeWeights, weights)(
                  op.candidates, candidates)
          .result
      }
      val newLeftName = graph_operations.PulledOverVertexAttribute.pullAttributeVia(
        leftName, reverse(fingerprinting.matching))
      val newRightName = graph_operations.PulledOverVertexAttribute.pullAttributeVia(
        rightName, fingerprinting.matching)

      project.scalars("fingerprinting matches found") = count(fingerprinting.matching)
      project.vertexAttributes(params("leftName")) = unifyAttribute(newLeftName, leftName)
      project.vertexAttributes(params("rightName")) = unifyAttribute(newRightName, rightName)
      project.vertexAttributes(params("leftName") + " similarity score") = fingerprinting.leftSimilarities
      project.vertexAttributes(params("rightName") + " similarity score") = fingerprinting.rightSimilarities
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Copy vertex attributes from segmentation"
    val description =
      "Copies all vertex attributes from the segmentation to the parent."
    def parameters = List(
      Param("prefix", "Attribute name prefix", defaultValue = seg.name))
    def enabled =
      FEStatus.assert(vertexAttributes.size > 0, "No vertex attributes") &&
        FEStatus.assert(parent.vertexSet != null, s"No vertices on $parent") &&
        FEStatus.assert(seg.belongsTo.properties.isFunction,
          s"Vertices of $parent are not guaranteed to have only one edge to this segmentation")
    def apply(params: Map[String, String]): Unit = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((name, attr) <- project.vertexAttributes.toMap) {
        parent.vertexAttributes(prefix + name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(
            attr, seg.belongsTo)
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Copy vertex attributes to segmentation"
    val description =
      "Copies all vertex attributes from the parent to the segmentation."
    def parameters = List(
      Param("prefix", "Attribute name prefix"))
    def enabled =
      hasVertexSet &&
        FEStatus.assert(parent.vertexAttributes.size > 0, "No vertex attributes on $parent") &&
        FEStatus.assert(seg.belongsTo.properties.isReversedFunction,
          s"Vertices of this segmentation are not guaranteed to have only one edge from $parent")
    def apply(params: Map[String, String]): Unit = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((name, attr) <- parent.vertexAttributes.toMap) {
        project.vertexAttributes(prefix + name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(
            attr, reverse(seg.belongsTo))
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Fingerprinting between project and segmentation"
    val description =
      """Finds the best match out of the potential matches that are defined between a project and
      a segmentation. The best match is chosen by comparing the vertex neighborhoods in the project
      and the segmentation.

      <p>The result of this operation is an updated edge set between the project and the
      segmentation, that is a one-to-one matching.

      <p>Example use-case: Project M is an MSISDN graph based on call data. Project F is a Facebook
      graph. A CSV file contains a number of MSISDN -> Facebook ID mappings, a many-to-many
      relationship. Connect the two projects with "Import project as segmentation", then use this
      operation to turn the mapping into a high-quality one-to-one relationship.
      """
    def parameters = List(
      Param("mrew", "Minimum relative edge weight", defaultValue = "0.0"),
      Param("mo", "Minimum overlap", defaultValue = "1"),
      Param("ms", "Minimum similarity", defaultValue = "0.5"))
    def enabled =
      hasEdgeBundle && FEStatus.assert(parent.edgeBundle != null, s"No edges on $parent")
    def apply(params: Map[String, String]): Unit = {
      val mrew = params("mrew").toDouble
      val mo = params("mo").toInt
      val ms = params("ms").toDouble

      // TODO: Calculate relative edge weight, filter the edge bundle and pull over the weights.
      assert(mrew == 0, "Minimum relative edge weight is not implemented yet.")

      val candidates = seg.belongsTo
      val segNeighborsInParent = concat(project.edgeBundle, reverse(seg.belongsTo))
      val fingerprinting = {
        val op = graph_operations.Fingerprinting(mo, ms)
        op(
          op.leftEdges, parent.edgeBundle)(
            op.leftEdgeWeights, const(parent.edgeBundle))(
              op.rightEdges, segNeighborsInParent)(
                op.rightEdgeWeights, const(segNeighborsInParent))(
                  op.candidates, candidates)
          .result
      }

      project.scalars("fingerprinting matches found") = count(fingerprinting.matching)
      seg.belongsTo = fingerprinting.matching
      parent.vertexAttributes("fingerprinting_similarity_score") = fingerprinting.leftSimilarities
      project.vertexAttributes("fingerprinting_similarity_score") = fingerprinting.rightSimilarities
    }
  })

  register(new UtilityOperation(_) {
    val title = "Change project notes"
    val description = ""
    def parameters = List(
      Param("notes", "New contents"))
    def enabled = FEStatus.enabled
    def apply(params: Map[String, String]) = {
      project.notes = params("notes")
    }
  })

  register(new SegmentationWorkflowOperation(_) {
    val title = "Viral modeling"
    val description = """Viral modeling tries to predict unknown values of an attribute based on
        the known values of the attribute on peers that belong to the same segments."""
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "viral"),
      Param("target", "Target attribute",
        options = UIValue.list(parentDoubleAttributes)),
      Param("test_set_ratio", "Test set ratio", defaultValue = "0.1"),
      Param("max_deviation", "Maximal segment deviation", defaultValue = "1.0"),
      Param("seed", "Seed", defaultValue = "0"),
      Param("iterations", "Iterations", defaultValue = "3"),
      Param("min_num_defined", "Minimum number of defined attributes in a segment", defaultValue = "6"),
      Param("min_ratio_defined", "Minimal ratio of defined attributes in a segment", defaultValue = "0.5"))
    def parentDoubleAttributes = parent.vertexAttributeNames[Double].toList
    def enabled = hasVertexSet &&
      FEStatus.assert(UIValue.list(parentDoubleAttributes).nonEmpty,
        "No numeric vertex attributes.")
    def apply(params: Map[String, String]) = {
      // partition target attribute to test and train sets
      val targetName = params("target")
      val target = parent.vertexAttributes(targetName).runtimeSafeCast[Double]
      val roles = {
        val op = graph_operations.CreateRole(params("test_set_ratio").toDouble, params("seed").toInt)
        op(op.vertices, target.vertexSet).result.role
      }
      val parted = {
        val op = graph_operations.PartitionAttribute[Double]()
        op(op.attr, target)(op.role, roles).result
      }
      val prefix = params("prefix")
      parent.vertexAttributes(s"${prefix}_roles") = roles
      parent.vertexAttributes(s"${prefix}_${targetName}_test") = parted.test
      var train = parted.train.entity
      val segSizes = computeSegmentSizes(seg)
      project.vertexAttributes("size") = segSizes
      val maxDeviation = params("max_deviation")

      val coverage = {
        val op = graph_operations.CountAttributes[Double]()
        op(op.attribute, train).result.count
      }
      parent.vertexAttributes(s"${prefix}_${targetName}_train") = train
      parent.scalars(s"$prefix $targetName coverage initial") = coverage

      var timeOfDefinition = {
        val op = graph_operations.DeriveJSDouble(JavaScript("0"), Seq("attr"))
        op(op.attrs, graph_operations.VertexAttributeToJSValue.seq(train)).result.attr.entity
      }

      // iterative prediction
      for (i <- 1 to params("iterations").toInt) {
        val segTargetAvg = {
          aggregateViaConnection(
            seg.belongsTo,
            AttributeWithLocalAggregator(train, "average"))
            .runtimeSafeCast[Double]
        }
        val segStdDev = {
          aggregateViaConnection(
            seg.belongsTo,
            AttributeWithLocalAggregator(train, "std_deviation"))
            .runtimeSafeCast[Double]
        }
        val segTargetCount = {
          aggregateViaConnection(
            seg.belongsTo,
            AttributeWithLocalAggregator(train, "count"))
            .runtimeSafeCast[Double]
        }
        val segStdDevDefined = {
          val op = graph_operations.DeriveJSDouble(
            JavaScript(s"""
                deviation <= $maxDeviation &&
                defined / ids >= ${params("min_ratio_defined")} &&
                defined >= ${params("min_num_defined")}
                ? deviation
                : undefined"""),
            Seq("deviation", "ids", "defined"))
          op(
            op.attrs,
            graph_operations.VertexAttributeToJSValue.seq(segStdDev, segSizes, segTargetCount))
            .result.attr
        }
        project.vertexAttributes(s"${prefix}_${targetName}_standard_deviation_after_iteration_$i") =
          segStdDev
        project.vertexAttributes(s"${prefix}_${targetName}_average_after_iteration_$i") =
          segTargetAvg // TODO: use median
        val predicted = {
          aggregateViaConnection(
            reverse(seg.belongsTo),
            AttributeWithWeightedAggregator(segStdDevDefined, segTargetAvg, "by_min_weight"))
            .runtimeSafeCast[Double]
        }
        train = unifyAttributeT(train, predicted)
        val partedTrain = {
          val op = graph_operations.PartitionAttribute[Double]()
          op(op.attr, train)(op.role, roles).result
        }
        val error = {
          val op = graph_operations.DeriveJSDouble(
            JavaScript("Math.abs(test - train)"), Seq("test", "train"))
          val mae = op(
            op.attrs,
            graph_operations.VertexAttributeToJSValue.seq(
              parted.test.entity, partedTrain.test.entity)).result.attr
          aggregate(AttributeWithAggregator(mae, "average"))
        }
        val coverage = {
          val op = graph_operations.CountAttributes[Double]()
          op(op.attribute, partedTrain.train).result.count
        }
        // the attribute we use for iteration can be defined on the test set as well
        parent.vertexAttributes(s"${prefix}_${targetName}_after_iteration_$i") = train
        parent.scalars(s"$prefix $targetName coverage after iteration $i") = coverage
        parent.scalars(s"$prefix $targetName mean absolute prediction error after iteration $i") =
          error

        timeOfDefinition = {
          val op = graph_operations.DeriveJSDouble(
            JavaScript(i.toString), Seq("attr"))
          val newDefinitions = op(
            op.attrs, graph_operations.VertexAttributeToJSValue.seq(train)).result.attr
          unifyAttributeT(timeOfDefinition, newDefinitions)
        }
      }
      parent.vertexAttributes(s"${prefix}_${targetName}_spread_over_iterations") = timeOfDefinition
      // TODO: in the end we should calculate with the fact that the real error where the
      // original attribute is defined is 0.0
    }
  })

  register(new AttributeOperation(_) {
    val title = "Correlate two attributes"
    val description = """Calculates correlation coefficient of two attributes."""
    def parameters = List(
      Param("attrA", "First attribute", options = vertexAttributes[Double]),
      Param("attrB", "Second attribute", options = vertexAttributes[Double]))
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes")
    def apply(params: Map[String, String]) = {
      val attrA = project.vertexAttributes(params("attrA")).runtimeSafeCast[Double]
      val attrB = project.vertexAttributes(params("attrB")).runtimeSafeCast[Double]
      val op = graph_operations.CorrelateAttributes()
      val res = op(op.attrA, attrA)(op.attrB, attrB).result
      project.scalars(s"correlation of ${params("attrA")} and ${params("attrB")}") =
        res.correlation
    }
  })

  register(new AttributeOperation(_) {
    val title = "Filter by attributes"
    val description =
      """Keeps only vertices and edges that match the filters. The filter syntax is documented in the
    <a href="http://rnd.lynxanalytics.com/lynxkite-user-guide">LynxKite User Guide</a>.
    """
    def parameters =
      vertexAttributes.toList.map { attr => Param(s"filterva-${attr.id}", attr.id) } ++
        project.segmentations.toList.map { seg => Param(s"filterva-${seg.equivalentUIAttribute.title}", seg.name) } ++
        edgeAttributes.toList.map { attr => Param(s"filterea-${attr.id}", attr.id) }
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes") ||
        FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    val vaFilter = "filterva-(.*)".r
    val eaFilter = "filterea-(.*)".r

    override def summary(params: Map[String, String]) = {
      val filterStrings = params.collect {
        case (vaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
        case (eaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
      }
      "Filter " + filterStrings.mkString(", ")
    }
    def apply(params: Map[String, String]) = {
      val vertexFilters = params.collect {
        case (vaFilter(name), filter) if filter.nonEmpty =>
          // The filter may be for a segmentation's equivalent attribute or for a vertex attribute.
          val segAttrs = project.segmentations.map(_.equivalentUIAttribute)
          val segGUIDOpt = segAttrs.find(_.title == name).map(_.id)
          val gUID = segGUIDOpt.getOrElse(project.vertexAttributes(name).gUID)
          FEVertexAttributeFilter(gUID.toString, filter)
      }.toSeq
      val edgeFilters = params.collect {
        case (eaFilter(name), filter) if filter.nonEmpty =>
          val attr = project.edgeAttributes(name)
          FEVertexAttributeFilter(attr.gUID.toString, filter)
      }.toSeq
      assert(vertexFilters.nonEmpty || edgeFilters.nonEmpty, "No filters specified.")

      if (vertexFilters.nonEmpty) {
        val vertexEmbedding = FEFilters.embedFilteredVertices(
          project.vertexSet, vertexFilters, heavy = true)
        project.pullBack(vertexEmbedding)
      }
      if (edgeFilters.nonEmpty) {
        val edgeEmbedding = FEFilters.embedFilteredVertices(
          project.edgeBundle.idSet, edgeFilters, heavy = true)
        project.pullBackEdges(edgeEmbedding)
      }
    }
  })

  register(new UtilityOperation(_) {
    val title = "Save UI status as graph attribute"
    val description =
      """Saves UI status as a graph attribute that can be reused
         later to reload the same visualization.
      """
    def parameters = List(
      // In the future we may want a special kind for this so that user's don't see JSON.
      Param("scalarName", "Name of new graph attribute"),
      Param("uiStatusJson", "UI status as JSON"))

    def enabled = FEStatus.enabled

    def apply(params: Map[String, String]) = {
      import UIStatusSerialization._
      val uiStatusJson = json.Json.parse(params("uiStatusJson"))
      val uiStatus = json.Json.fromJson[UIStatus](uiStatusJson).get
      project.scalars(params("scalarName")) =
        graph_operations.CreateUIStatusScalar(uiStatus).result.created
    }
  })

  { // "Dirty operations", that is operations that use a data manager. Think twice if you really
    // need this before putting an operation here.
    implicit val dataManager = env.dataManager

    register(new AttributeOperation(_) {
      val title = "Export vertex attributes to file"
      val description = ""
      def parameters = List(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "vertex_attributes_csv"),
        Param("attrs", "Attributes", options = vertexAttributes, multipleChoice = true),
        Param("format", "File format", options = UIValue.list(List("CSV", "SQL dump"))))
      def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "Nothing selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Map[String, Attribute[_]] = labels.map {
          label => label -> project.vertexAttributes(label)
        }.toMap
        val path = getExportFilename(params("path"))
        params("format") match {
          case "CSV" =>
            val csv = graph_util.CSVExport.exportVertexAttributes(project.vertexSet, attrs)
            csv.saveToDir(path)
          case "SQL dump" =>
            val export = graph_util.SQLExport(project.projectName, project.vertexSet, attrs)
            export.saveAs(path)
        }
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    register(new AttributeOperation(_) {
      val title = "Export vertex attributes to database"
      val description = """
        Creates a new table and writes the selected attributes into it.
        """ + jdbcHelpText
      def parameters = List(
        Param("db", "Database"),
        Param("table", "Table"),
        Param("attrs", "Attributes", options = vertexAttributes, multipleChoice = true),
        Param("delete", "Overwrite table if it exists", options = UIValue.list(List("no", "yes"))))
      def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "Nothing selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Seq[(String, Attribute[_])] = labels.map {
          label => label -> project.vertexAttributes(label)
        }
        val export = graph_util.SQLExport(params("table"), project.vertexSet, attrs.toMap)
        export.insertInto(params("db"), delete = params("delete") == "yes")
      }
    })

    def getExportFilename(param: String): Filename = {
      assert(param.nonEmpty, "No export path specified.")
      if (param == "<auto>") {
        dataManager.repositoryPath / "exports" / graph_util.Timestamp.toString
      } else {
        Filename(param)
      }
    }

    register(new AttributeOperation(_) {
      val title = "Export edge attributes to file"
      val description = ""
      def parameters = List(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "edge_attributes_csv"),
        Param("attrs", "Attributes", options = edgeAttributes, multipleChoice = true),
        Param("format", "File format", options = UIValue.list(List("CSV", "SQL dump"))))
      def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "Nothing selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Map[String, Attribute[_]] = labels.map {
          label => label -> project.edgeAttributes(label)
        }.toMap
        val path = getExportFilename(params("path"))
        params("format") match {
          case "CSV" =>
            val csv = graph_util.CSVExport.exportEdgeAttributes(project.edgeBundle, attrs)
            csv.saveToDir(path)
          case "SQL dump" =>
            val export = graph_util.SQLExport(project.projectName, project.edgeBundle, attrs)
            export.saveAs(path)
        }
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    register(new AttributeOperation(_) {
      val title = "Export edge attributes to database"
      val description = """
        Creates a new table and writes the selected attributes into it.
        """ + jdbcHelpText
      def parameters = List(
        Param("db", "Database"),
        Param("table", "Table"),
        Param("attrs", "Attributes", options = edgeAttributes, multipleChoice = true),
        Param("delete", "Overwrite table if it exists", options = UIValue.list(List("no", "yes"))))
      def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "Nothing selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Map[String, Attribute[_]] = labels.map {
          label => label -> project.edgeAttributes(label)
        }.toMap
        val export = graph_util.SQLExport(params("table"), project.edgeBundle, attrs)
        export.insertInto(params("db"), delete = params("delete") == "yes")
      }
    })

    register(new SegmentationOperation(_) {
      val title = "Export segmentation to file"
      val description = ""
      def parameters = List(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "segmentation_csv"),
        Param("format", "File format", options = UIValue.list(List("CSV", "SQL dump"))))
      def enabled = FEStatus.enabled
      def apply(params: Map[String, String]) = {
        val path = getExportFilename(params("path"))
        val name = project.asSegmentation.name
        params("format") match {
          case "CSV" =>
            val csv = graph_util.CSVExport.exportEdgeAttributes(
              seg.belongsTo, attributes = Map(),
              srcColumnName = "vertex_id", dstColumnName = s"${name}_id")
            csv.saveToDir(path)
          case "SQL dump" =>
            val export = graph_util.SQLExport(
              name, seg.belongsTo, attributes = Map[String, Attribute[_]](),
              srcColumnName = "vertex_id", dstColumnName = s"${name}_id")
            export.saveAs(path)
        }
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    register(new SegmentationOperation(_) {
      val title = "Export segmentation to database"
      val description = """
        Creates a new table and writes the edges going from the parent graph to this
        segmentation into it.""" + jdbcHelpText
      def parameters = List(
        Param("db", "Database"),
        Param("table", "Table"),
        Param("delete", "Overwrite table if it exists", options = UIValue.list(List("no", "yes"))))
      def enabled = FEStatus.enabled
      def apply(params: Map[String, String]) = {
        val export = graph_util.SQLExport(params("table"), seg.belongsTo, Map[String, Attribute[_]]())
        export.insertInto(params("db"), delete = params("delete") == "yes")
      }
    })
  }

  def joinAttr[A, B](a: Attribute[A], b: Attribute[B]): Attribute[(A, B)] = {
    graph_operations.JoinAttributes.run(a, b)
  }

  def computeSegmentSizes(segmentation: Segmentation): Attribute[Double] = {
    val op = graph_operations.OutDegree()
    op(op.es, reverse(segmentation.belongsTo)).result.outDegree
  }

  def toDouble(attr: Attribute[_]): Attribute[Double] = {
    if (attr.is[String])
      graph_operations.VertexAttributeToDouble.run(attr.runtimeSafeCast[String])
    else if (attr.is[Long])
      graph_operations.LongAttributeToDouble.run(attr.runtimeSafeCast[Long])
    else
      throw new AssertionError(s"Unexpected type (${attr.typeTag}) on $attr")
  }

  private def attributeToString[T](attr: Attribute[T]): Attribute[String] = {
    val op = graph_operations.VertexAttributeToString[T]()
    op(op.attr, attr).result.attr
  }

  def parseAggregateParams(params: Map[String, String]) = {
    val aggregate = "aggregate-(.*)".r
    params.collect {
      case (aggregate(attr), choice) if choice != "ignore" => attr -> choice
    }
  }
  def aggregateParams(
    attrs: Iterable[(String, Attribute[_])],
    needsGlobal: Boolean = false,
    weighted: Boolean = false): List[FEOperationParameterMeta] = {
    attrs.toList.map {
      case (name, attr) =>
        val options = if (attr.is[Double]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.list(List("ignore", "weighted_sum", "weighted_average", "by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.list(List("ignore", "sum", "average", "min", "max", "count", "first", "std_deviation"))
          } else {
            UIValue.list(List("ignore", "sum", "average", "min", "max", "most_common", "count", "vector", "std_deviation"))
          }
        } else if (attr.is[String]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.list(List("ignore", "by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.list(List("ignore", "count", "first"))
          } else {
            UIValue.list(List("ignore", "most_common", "majority_50", "majority_100", "count", "vector"))
          }
        } else {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.list(List("ignore", "by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.list(List("ignore", "count", "first"))
          } else {
            UIValue.list(List("ignore", "most_common", "count", "vector"))
          }
        }
        Param(s"aggregate-$name", name, options = options)
    }
  }

  // Performs AggregateAttributeToScalar.
  private def aggregate[From, Intermediate, To](
    attributeWithAggregator: AttributeWithAggregator[From, Intermediate, To]): Scalar[To] = {
    val op = graph_operations.AggregateAttributeToScalar(attributeWithAggregator.aggregator)
    op(op.attr, attributeWithAggregator.attr).result.aggregated
  }

  // Performs AggregateByEdgeBundle.
  private def aggregateViaConnection[From, To](
    connection: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateByEdgeBundle(attributeWithAggregator.aggregator)
    op(op.connection, connection)(op.attr, attributeWithAggregator.attr).result.attr
  }

  // Performs AggregateFromEdges.
  private def aggregateFromEdges[From, To](
    edges: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateFromEdges(attributeWithAggregator.aggregator)
    val res = op(op.edges, edges)(op.eattr, attributeWithAggregator.attr).result
    res.dstAttr
  }

  def reverse(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.ReverseEdges()
    op(op.esAB, eb).result.esBA
  }

  def removeNonSymmetric(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.RemoveNonSymmetricEdges()
    op(op.es, eb).result.symmetric
  }

  def addReversed(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.AddReversedEdges()
    op(op.es, eb).result.esPlus
  }

  object Direction {
    // Options suitable when edge attributes are involved.
    val attrOptions = UIValue.list(List(
      "incoming edges",
      "outgoing edges",
      "all edges"))
    // Options suitable when edge attributes are not involved.
    val options = attrOptions :+ UIValue("symmetric edges", "symmetric edges")
  }
  case class Direction(direction: String, origEB: EdgeBundle, reversed: Boolean = false) {
    val unchangedOut: (EdgeBundle, Option[EdgeBundle]) = (origEB, None)
    val reversedOut: (EdgeBundle, Option[EdgeBundle]) = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, origEB).result
      (res.esBA, Some(res.injection))
    }
    val (edgeBundle, pullBundleOpt): (EdgeBundle, Option[EdgeBundle]) = direction match {
      case "incoming edges" => if (reversed) reversedOut else unchangedOut
      case "outgoing edges" => if (reversed) unchangedOut else reversedOut
      case "all edges" =>
        val op = graph_operations.AddReversedEdges()
        val res = op(op.es, origEB).result
        (res.esPlus, Some(res.newToOriginal))
      case "symmetric edges" =>
        // Use "null" as the injection because it is an error to use
        // "symmetric edges" with edge attributes.
        (removeNonSymmetric(origEB), Some(null))
    }

    def pull[T](attribute: Attribute[T]): Attribute[T] = {
      pullBundleOpt.map { pullBundle =>
        graph_operations.PulledOverVertexAttribute.pullAttributeVia(attribute, pullBundle)
      }.getOrElse(attribute)
    }
  }

  def count(eb: EdgeBundle): Scalar[Long] = {
    val op = graph_operations.CountEdges()
    op(op.edges, eb).result.count
  }

  private def unifyAttributeT[T](a1: Attribute[T], a2: Attribute[_]): Attribute[T] = {
    val op = graph_operations.AttributeFallback[T]()
    op(op.originalAttr, a1)(op.defaultAttr, a2.runtimeSafeCast(a1.typeTag)).result.defaultedAttr
  }
  def unifyAttribute(a1: Attribute[_], a2: Attribute[_]): Attribute[_] = {
    unifyAttributeT(a1, a2)
  }

  def unifyAttributes(
    as1: Iterable[(String, Attribute[_])],
    as2: Iterable[(String, Attribute[_])]): Map[String, Attribute[_]] = {

    val m1 = as1.toMap
    val m2 = as2.toMap
    m1.keySet.union(m2.keySet)
      .map(k => k -> (m1.get(k) ++ m2.get(k)).reduce(unifyAttribute _))
      .toMap
  }

  def concat(eb1: EdgeBundle, eb2: EdgeBundle): EdgeBundle = {
    new graph_util.BundleChain(Seq(eb1, eb2)).getCompositeEdgeBundle._1
  }

  def const(eb: EdgeBundle, value: Double = 1.0): Attribute[Double] = {
    graph_operations.AddConstantAttribute.run(eb.idSet, value)
  }

  def newScalar(data: String): Scalar[String] = {
    val op = graph_operations.CreateStringScalar(data)
    op.result.created
  }

  def downloadLink(fn: Filename, name: String) = {
    val urlPath = java.net.URLEncoder.encode(fn.fullString, "utf-8")
    val urlName = java.net.URLEncoder.encode(name, "utf-8")
    val url = s"/download?path=$urlPath&name=$urlName"
    val quoted = '"' + url + '"'
    newScalar(s"<a href=$quoted>download</a>")
  }
}
