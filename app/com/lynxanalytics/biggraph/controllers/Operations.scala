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
import scala.reflect.runtime.universe.typeOf

class Operations(env: BigGraphEnvironment) extends OperationRepository(env) {
  val Param = FEOperationParameterMeta // Short alias.

  // Categories.
  abstract class VertexOperation(p: Project) extends Operation(p, "Vertex operations")
  abstract class EdgeOperation(p: Project) extends Operation(p, "Edge operations")
  abstract class AttributeOperation(p: Project) extends Operation(p, "Attribute operations")
  abstract class SegmentationOperation(p: Project) extends Operation(p, "Segmentation operations")
  abstract class HiddenOperation(p: Project) extends Operation(p, "<hidden>")

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
      Param("files", "Files", kind = "file"),
      Param("header", "Header", defaultValue = "<read first line>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("filter", "(optional) Filtering expression"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val files = Filename.fromString(params("files"))
      val header = if (params("header") == "<read first line>")
        graph_operations.ImportUtil.header(files) else params("header")
      val csv = graph_operations.CSV(
        files,
        params("delimiter"),
        header,
        JavaScript(params("filter")))
      val imp = graph_operations.ImportVertexList(csv)().result
      project.vertexSet = imp.vertices
      project.vertexAttributes = imp.attrs.mapValues(_.entity)
      FEStatus.success
    }
  })

  register(new EdgeOperation(_) {
    val title = "Import edges for existing vertices"
    val parameters = Seq(
      Param("files", "Files", kind = "file"),
      Param("header", "Header", defaultValue = "<read first line>"),
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
      val files = Filename.fromString(params("files"))
      val header = if (params("header") == "<read first line>")
        graph_operations.ImportUtil.header(files) else params("header")
      val csv = graph_operations.CSV(
        files,
        params("delimiter"),
        header,
        JavaScript(params("filter")))
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
      Param("files", "Files", kind = "file"),
      Param("header", "Header", defaultValue = "<read first line>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"),
      Param("filter", "(optional) Filtering expression"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val files = Filename.fromString(params("files"))
      val header = if (params("header") == "<read first line>")
        graph_operations.ImportUtil.header(files) else params("header")
      val csv = graph_operations.CSV(
        files,
        params("delimiter"),
        header,
        JavaScript(params("filter")))
      val src = params("src")
      val dst = params("dst")
      val imp = graph_operations.ImportEdgeList(csv, src, dst)().result
      project.vertexSet = imp.vertices
      project.edgeBundle = imp.edges
      project.edgeAttributes = imp.attrs.mapValues(_.entity)
      project.vertexAttributes("stringID") = imp.stringID
      FEStatus.success
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Maximal cliques"
    val parameters = Seq(
      Param("name", "Segmentation name", defaultValue = "maximal_cliques"),
      Param("bothdir", "Edges required in both directions", options = UIValue.seq(Seq("true", "false"))),
      Param("min", "Minimum clique size", defaultValue = "3"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FindMaxCliques(params("min").toInt, params("bothdir").toBoolean)
      val result = op(op.es, project.edgeBundle).result
      val segmentation = project.segmentation(params("name"))
      segmentation.project.vertexSet = result.segments
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
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
      val result = op(op.es, project.edgeBundle).result
      val segmentation = project.segmentation(params("name"))
      segmentation.project.vertexSet = result.segments
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
      FEStatus.success
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Find infocom communities"
    val parameters = Seq(
      Param(
        "cliques_name", "Name for maximal cliques segmentation", defaultValue = "maximal_cliques"),
      Param(
        "communities_name", "Name for communities segmentation", defaultValue = "communities"),
      Param("bothdir", "Edges required in cliques in both directions", defaultValue = "true"),
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
      cliquesSegmentation.project.vertexSet = cliquesResult.segments
      cliquesSegmentation.project.notes = "Maximal cliques of %s".format(project.projectName)
      cliquesSegmentation.belongsTo = cliquesResult.belongsTo
      computeSegmentSizes(cliquesSegmentation)

      val cedges = {
        val op = graph_operations.InfocomOverlapForCC(params("adjacency_threshold").toDouble)
        op(op.belongsTo, cliquesResult.belongsTo).result.overlaps
      }

      val ccResult = {
        val op = graph_operations.ConnectedComponents()
        op(op.es, cedges).result
      }

      val (weightedVertexToClique, weightedCliqueToCommunity) = {
        val op = graph_operations.AddConstantDoubleEdgeAttribute(1.0)
        (op(op.edges, cliquesResult.belongsTo).result.attr,
          op(op.edges, ccResult.belongsTo).result.attr)
      }

      val weightedVertexToCommunity = {
        val op = graph_operations.ConcatenateBundles()
        op(op.weightsAB, weightedVertexToClique)(op.weightsBC, weightedCliqueToCommunity)
          .result.weightsAC
      }

      val communitiesSegmentation = project.segmentation(params("communities_name"))
      communitiesSegmentation.project.vertexSet = ccResult.segments
      communitiesSegmentation.project.notes =
        "Infocom Communities of %s".format(project.projectName)
      communitiesSegmentation.belongsTo = weightedVertexToCommunity.edgeBundle
      computeSegmentSizes(communitiesSegmentation)

      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add gaussian vertex attribute"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "random"),
      Param("seed", "Seed", defaultValue = "0"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val op = graph_operations.AddGaussianVertexAttribute(params("seed").toInt)
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
      project.edgeBundle = reverse(project.edgeBundle)
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
    val title = "Degree"
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "degree"),
      Param("inout", "Type", options = UIValue.seq(Seq("in", "out", "all", "symmetric"))))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val es = project.edgeBundle
      val esSym = {
        val op = graph_operations.RemoveNonSymmetricEdges()
        op(op.es, es).result.symmetric
      }
      val deg = params("inout") match {
        case "in" => applyOn(reverse(es))
        case "out" => applyOn(es)
        case "symmetric" => applyOn(esSym)
        case "all" => graph_operations.DeriveJS.add(applyOn(reverse(es)), applyOn(es))
      }
      project.vertexAttributes(params("name")) = deg
      FEStatus.success
    }

    private def applyOn(es: EdgeBundle): VertexAttribute[Double] = {
      val op = graph_operations.OutDegree()
      op(op.es, es).result.outDegree
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
      Param("attr", "Vertex attribute", options = vertexAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
    private def applyOn[T](attr: VertexAttribute[T]) = {
      val op = graph_operations.VertexAttributeToString[T]()
      op(op.attr, attr).result.attr
    }
    def apply(params: Map[String, String]): FEStatus = {
      for (attr <- params("attr").split(",")) {
        project.vertexAttributes(attr) = applyOn(project.vertexAttributes(attr))
      }
      FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Vertex attribute to double"
    val parameters = Seq(
      Param("attr", "Vertex attribute", options = vertexAttributes[String], multipleChoice = true))
    def enabled = FEStatus.assert(vertexAttributes[String].nonEmpty, "No vertex attributes.")
    def apply(params: Map[String, String]): FEStatus = {
      for (name <- params("attr").split(",")) {
        val attr = project.vertexAttributes(name).runtimeSafeCast[String]
        project.vertexAttributes(name) = toDouble(attr)
      }
      FEStatus.success
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

  register(new AttributeOperation(_) {
    val title = "Derived vertex attribute"
    val parameters = Seq(
      Param("output", "Save as"),
      Param("expr", "Value", defaultValue = "1"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]): FEStatus = {
      val expr = params("expr")
      var numAttrNames = List[String]()
      var numAttrs = List[VertexAttribute[Double]]()
      var strAttrNames = List[String]()
      var strAttrs = List[VertexAttribute[String]]()
      project.vertexAttributes.foreach {
        case (name, attr) if expr.contains(name) && attr.is[Double] =>
          numAttrNames +:= name
          numAttrs +:= attr.asInstanceOf[VertexAttribute[Double]]
        case (name, attr) if expr.contains(name) && attr.is[String] =>
          strAttrNames +:= name
          strAttrs +:= attr.asInstanceOf[VertexAttribute[String]]
        case (name, attr) if expr.contains(name) =>
          log.warn(s"'$name' is of an unsupported type: ${attr.typeTag.tpe}")
        case _ => ()
      }
      val js = JavaScript(expr)
      // Figure out the return type.
      val op: graph_operations.DeriveJS[_] = testEvaluation(js, numAttrNames, strAttrNames) match {
        case _: String =>
          graph_operations.DeriveJSString(js, numAttrNames, strAttrNames)
        case _: Double =>
          graph_operations.DeriveJSDouble(js, numAttrNames, strAttrNames)
        case result =>
          return FEStatus.failure(s"Test evaluation of '$js' returned '$result'.")
      }
      val result = op(
        op.vs, project.vertexSet)(
          op.numAttrs, numAttrs)(
            op.strAttrs, strAttrs).result
      project.vertexAttributes(params("output")) = result.attr
      return FEStatus.success
    }

    // Evaluates the expression with 0/'' parameters.
    def testEvaluation(js: JavaScript, numAttrNames: Seq[String], strAttrNames: Seq[String]): Any = {
      val mapping = numAttrNames.map(_ -> 0.0).toMap ++ strAttrNames.map(_ -> "").toMap
      return js.evaluate(mapping)
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Aggregate to segmentation"
    val parameters = Seq(
      Param("segmentation", "Destination segmentation", options = segmentations)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes") &&
        FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]): FEStatus = {
      val seg = project.segmentation(params("segmentation"))
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          seg.belongsTo,
          attributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        seg.project.vertexAttributes(s"${attr}_${choice}") = result
      }
      return FEStatus.success
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Aggregate from segmentation"
    val parameters = Seq(
      Param("prefix", "Generated name prefix",
        defaultValue = if (project.isSegmentation) project.asSegmentation.name else "")) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(project.isSegmentation, "Operates on a segmentation") &&
        FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]): FEStatus = {
      val seg = project.asSegmentation
      val prefix = params("prefix")
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          reverse(seg.belongsTo),
          attributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        seg.parent.vertexAttributes(s"${prefix}_${attr}_${choice}") = result
      }
      return FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate on neighbors"
    val parameters = Seq(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Param("direction", "Aggregate on",
        options = UIValue.seq(Seq("incoming edges", "outgoing edges")))) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes") && hasEdgeBundle
    def apply(params: Map[String, String]): FEStatus = {
      val prefix = params("prefix")
      val edges = params("direction") match {
        case "incoming edges" => project.edgeBundle
        case "outgoing edges" => reverse(project.edgeBundle)
      }
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          edges,
          attributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${prefix}_${attr}_${choice}") = result
      }
      return FEStatus.success
    }
  })

  register(new VertexOperation(_) {
    val title = "Merge vertices by attribute"
    val parameters = Seq(
      Param("key", "Match by", options = vertexAttributes)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def merge[T](attr: VertexAttribute[T]): graph_operations.Segmentation = {
      val op = graph_operations.MergeVertices[T]()
      op(op.attr, attr).result
    }
    def apply(params: Map[String, String]): FEStatus = {
      val m = merge(project.vertexAttributes(params("key")))
      val oldAttrs = project.vertexAttributes.toMap
      val oldEdges = project.edgeBundle
      project.vertexSet = m.segments
      // Always use most_common for the key attribute.
      val hack = "aggregate-" + params("key") -> "most_common"
      for ((attr, choice) <- parseAggregateParams(params + hack)) {
        val result = aggregateViaConnection(
          m.belongsTo,
          attributeWithLocalAggregator(oldAttrs(attr), choice))
        project.vertexAttributes(attr) = result
      }
      project.edgeBundle = {
        val op = graph_operations.InducedEdgeBundle()
        op(op.srcMapping, m.belongsTo)(op.dstMapping, m.belongsTo)(op.edges, oldEdges).result.induced
      }
      return FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate vertex attribute"
    val parameters = Seq(Param("prefix", "Generated name prefix", defaultValue = "")) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]): FEStatus = {
      val prefix = params("prefix")
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(attributeWithAggregator(project.vertexAttributes(attr), choice))
        val name = if (prefix.isEmpty) s"${attr}_${choice}" else s"${prefix}_${attr}_${choice}"
        project.scalars(name) = result
      }
      return FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate edge attribute"
    val parameters = Seq(Param("prefix", "Generated name prefix", defaultValue = "")) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea.asVertexAttribute) },
        needsGlobal = true)
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]): FEStatus = {
      val prefix = params("prefix")
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          attributeWithAggregator(project.edgeAttributes(attr).asVertexAttribute, choice))
        val name = if (prefix.isEmpty) s"${attr}_${choice}" else s"${prefix}_${attr}_${choice}"
        project.scalars(name) = result
      }
      return FEStatus.success
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate edge attribute to vertices"
    val parameters = Seq(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Param("direction", "Aggregate on",
        options = UIValue.seq(Seq("incoming edges", "outgoing edges")))) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea.asVertexAttribute) })
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]): FEStatus = {
      val prefix = params("prefix")
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          project.edgeBundle,
          params("direction") == "outgoing edges",
          attributeWithLocalAggregator(
            project.edgeAttributes(attr).asVertexAttribute,
            choice))
        project.vertexAttributes(s"${prefix}_${attr}_${choice}") = result
      }
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard edge attribute"
    val parameters = Seq(
      Param("name", "Name", options = edgeAttributes))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]): FEStatus = {
      project.edgeAttributes(params("name")) = null
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard vertex attribute"
    val parameters = Seq(
      Param("name", "Name", options = vertexAttributes))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]): FEStatus = {
      project.vertexAttributes(params("name")) = null
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard segmentation"
    val parameters = Seq(
      Param("name", "Name", options = segmentations))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]): FEStatus = {
      project.segmentation(params("name")).remove
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard scalar"
    val parameters = Seq(
      Param("name", "Name", options = scalars))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    def apply(params: Map[String, String]): FEStatus = {
      project.scalars(params("name")) = null
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename edge attribute"
    val parameters = Seq(
      Param("from", "Old name", options = edgeAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]): FEStatus = {
      project.edgeAttributes(params("to")) = project.edgeAttributes(params("from"))
      project.edgeAttributes(params("from")) = null
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename vertex attribute"
    val parameters = Seq(
      Param("from", "Old name", options = vertexAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]): FEStatus = {
      project.vertexAttributes(params("to")) = project.vertexAttributes(params("from"))
      project.vertexAttributes(params("from")) = null
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename segmentation"
    val parameters = Seq(
      Param("from", "Old name", options = segmentations),
      Param("to", "New name"))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]): FEStatus = {
      project.segmentation(params("from")).rename(params("to"))
      return FEStatus.success
    }
  })

  register(new VertexOperation(_) {
    val title = "Union with another project"
    val parameters = Seq(
      Param("other", "Other project's name", options = uIProjects))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]): FEStatus = {
      val other = Project(params("other"))
      if (other.vertexSet == null) {
        // Nothing to do
        return FEStatus.success
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
          (ebInduced.get.induced.entity, ebInduced.get.embedding, null)
        } else if (!ebInduced.isDefined && otherEbInduced.isDefined) {
          (otherEbInduced.get.induced.entity, null, otherEbInduced.get.embedding)
        } else {
          assert(ebInduced.isDefined && otherEbInduced.isDefined)
          val idUnion = {
            val op = graph_operations.VertexSetUnion(2)
            op(
              op.vss,
              Seq(ebInduced.get.induced.asVertexSet, otherEbInduced.get.induced.asVertexSet))
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
        }
      val newEdgeAttributes = unifyAttributes(
        project.edgeAttributes
          .map {
            case (name, attr) => {
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr.asVertexAttribute,
                myEbInjection)
            }
          },
        other.edgeAttributes
          .map {
            case (name, attr) =>
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr.asVertexAttribute,
                otherEbInjection)
          })
        .mapValues(va => graph_operations.VertexAttributeAsEdgeAttribute.run(va, newEdgeBundle))

      project.vertexSet = vsUnion.union
      project.vertexAttributes = newVertexAttributes
      project.edgeBundle = newEdgeBundle
      project.edgeAttributes = newEdgeAttributes
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename scalar"
    val parameters = Seq(
      Param("from", "Old name", options = scalars),
      Param("to", "New name"))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    def apply(params: Map[String, String]): FEStatus = {
      project.scalars(params("to")) = project.scalars(params("from"))
      project.scalars(params("from")) = null
      return FEStatus.success
    }
  })

  register(new HiddenOperation(_) {
    val title = "Change project notes"
    val parameters = Seq(
      Param("notes", "New contents"))
    def enabled = FEStatus.success
    def apply(params: Map[String, String]): FEStatus = {
      project.notes = params("notes")
      return FEStatus.success
    }
  })

  { // "Dirty operations", that is operations that use a data manager. Think twice if you really
    // need this before putting an operation here.
    implicit val dataManager = env.dataManager

    register(new AttributeOperation(_) {
      val title = "Export vertex attributes to CSV"
      val parameters = Seq(
        Param("path", "Destination path"),
        Param("single", "Export as single csv", options = UIValue.seq(Seq("false", "true"))),
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
        val csv =
          graph_util.CSVExport
            .exportVertexAttributes(attrs, labels)
        if (params("single") == "true") {
          csv.saveToSingleFile(path)
        } else {
          csv.saveToDir(path)
        }
        return FEStatus.success
      }
    })

    register(new AttributeOperation(_) {
      val title = "Export edge attributes to CSV"
      val parameters = Seq(
        Param("path", "Destination path"),
        Param("single", "Export as single csv", options = UIValue.seq(Seq("false", "true"))),
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
        val csv = graph_util.CSVExport
          .exportEdgeAttributes(attrs, labels)
        if (params("single") == "true") {
          csv.saveToSingleFile(path)
        } else {
          csv.saveToDir(path)
        }
        return FEStatus.success
      }
    })

    register(new SegmentationOperation(_) {
      val title = "Export segmentation to CSV"
      val parameters = Seq(
        Param("path", "Destination path"),
        Param("single", "Export as single csv", options = UIValue.seq(Seq("false", "true"))),
        Param("seg", "Segmentation", options = segmentations))
      def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations.")
      def apply(params: Map[String, String]): FEStatus = {
        if (params("seg").isEmpty)
          return FEStatus.failure("Nothing selected for export.")
        val path = Filename.fromString(params("path"))
        val seg = project.segmentation(params("seg"))
        val csv = graph_util.CSVExport
          .exportEdgeAttributes(seg.belongsTo, Seq(), Seq())
        if (params("single") == "true") {
          csv.saveToSingleFile(path)
        } else {
          csv.saveToDir(path)
        }
        return FEStatus.success
      }
    })
  }

  def computeSegmentSizes(segmentation: Segmentation, attributeName: String = "size"): Unit = {
    val reversed = {
      val op = graph_operations.ReverseEdges()
      op(op.esAB, segmentation.belongsTo).result.esBA
    }

    segmentation.project.vertexAttributes(attributeName) = {
      val op = graph_operations.OutDegree()
      op(op.es, reversed).result.outDegree
    }
  }

  def toDouble(attr: VertexAttribute[String]): VertexAttribute[Double] = {
    val op = graph_operations.VertexAttributeToDouble()
    op(op.attr, attr).result.attr
  }

  def parseAggregateParams(params: Map[String, String]) = {
    val aggregate = "aggregate-(.*)".r
    params.collect {
      case (aggregate(attr), choice) if choice != "ignore" => attr -> choice
    }
  }
  def aggregateParams(
    attrs: Iterable[(String, VertexAttribute[_])],
    needsGlobal: Boolean = false): Seq[FEOperationParameterMeta] = {
    attrs.toSeq.map {
      case (name, attr) =>
        val options = if (attr.is[Double]) {
          if (needsGlobal) {
            UIValue.seq(Seq("ignore", "sum", "average", "min", "max", "count", "first"))
          } else {
            UIValue.seq(Seq("ignore", "sum", "average", "min", "max", "most_common", "count"))
          }
        } else if (attr.is[String]) {
          if (needsGlobal) {
            UIValue.seq(Seq("ignore", "count", "first"))
          } else {
            UIValue.seq(Seq("ignore", "most_common", "majority_50", "majority_100", "count"))
          }
        } else {
          if (needsGlobal) {
            UIValue.seq(Seq("ignore", "count", "first"))
          } else {
            UIValue.seq(Seq("ignore", "most_common", "count"))
          }
        }
        Param(s"aggregate-$name", name, options = options)
    }
  }

  trait AttributeWithLocalAggregator[From, To] {
    val attr: VertexAttribute[From]
    val aggregator: graph_operations.LocalAggregator[From, To]
  }
  object AttributeWithLocalAggregator {
    def apply[From, To](
      attrInp: VertexAttribute[From],
      aggregatorInp: graph_operations.LocalAggregator[From, To]): AttributeWithLocalAggregator[From, To] = {
      new AttributeWithLocalAggregator[From, To] {
        val attr = attrInp
        val aggregator = aggregatorInp
      }
    }
  }

  case class AttributeWithAggregator[From, Intermediate, To](
    val attr: VertexAttribute[From],
    val aggregator: graph_operations.Aggregator[From, Intermediate, To])
      extends AttributeWithLocalAggregator[From, To]

  private def attributeWithAggregator[T](
    attr: VertexAttribute[T], choice: String): AttributeWithAggregator[_, _, _] = {

    choice match {
      case "sum" => AttributeWithAggregator(attr.runtimeSafeCast[Double], graph_operations.Aggregator.Sum())
      case "count" => AttributeWithAggregator(attr, graph_operations.Aggregator.Count[T]())
      case "min" => AttributeWithAggregator(attr.runtimeSafeCast[Double], graph_operations.Aggregator.Min())
      case "max" => AttributeWithAggregator(attr.runtimeSafeCast[Double], graph_operations.Aggregator.Max())
      case "average" => AttributeWithAggregator(
        attr.runtimeSafeCast[Double], graph_operations.Aggregator.Average())
      case "first" => AttributeWithAggregator(attr, graph_operations.Aggregator.First[T]())
    }
  }

  private def attributeWithLocalAggregator[T](
    attr: VertexAttribute[T], choice: String): AttributeWithLocalAggregator[_, _] = {
    choice match {
      case "most_common" => AttributeWithLocalAggregator(attr, graph_operations.Aggregator.MostCommon[T]())
      case "majority_50" => AttributeWithLocalAggregator(attr.runtimeSafeCast[String], graph_operations.Aggregator.Majority(0.5))
      case "majority_100" => AttributeWithLocalAggregator(attr.runtimeSafeCast[String], graph_operations.Aggregator.Majority(1.0))
      case _ => attributeWithAggregator(attr, choice)
    }
  }

  // Performs AggregateAttributeToScalar.
  private def aggregate[From, Intermediate, To](
    attributeWithAggregator: AttributeWithAggregator[From, Intermediate, To]): Scalar[To] = {
    val op = graph_operations.AggregateAttributeToScalar(attributeWithAggregator.aggregator)
    op(op.attr, attributeWithAggregator.attr).result.aggregated
  }

  // Performs AggregateByEdgeBundle.
  def aggregateViaConnection[From, To](
    connection: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): VertexAttribute[To] = {
    val op = graph_operations.AggregateByEdgeBundle(attributeWithAggregator.aggregator)
    op(op.connection, connection)(op.attr, attributeWithAggregator.attr).result.attr
  }

  // Performs AggregateFromEdges.
  def aggregateFromEdges[From, To](
    edges: EdgeBundle,
    onSrc: Boolean,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): VertexAttribute[To] = {
    val op = graph_operations.AggregateFromEdges(attributeWithAggregator.aggregator)
    val res = op(op.edges, edges)(op.eattr, attributeWithAggregator.attr).result
    if (onSrc) res.srcAttr else res.dstAttr
  }

  def reverse(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.ReverseEdges()
    op(op.esAB, eb).result.esBA
  }

  def unifyAttributeT[T](a1: VertexAttribute[T], a2: VertexAttribute[_]): VertexAttribute[T] = {
    val op = graph_operations.AttributeFallback[T]()
    op(op.originalAttr, a1)(op.defaultAttr, a2.runtimeSafeCast(a1.typeTag)).result.defaultedAttr
  }
  def unifyAttribute(a1: VertexAttribute[_], a2: VertexAttribute[_]): VertexAttribute[_] = {
    unifyAttributeT(a1, a2)
  }

  def unifyAttributes(
    as1: Iterable[(String, VertexAttribute[_])],
    as2: Iterable[(String, VertexAttribute[_])]): Map[String, VertexAttribute[_]] = {

    val m1 = as1.toMap
    val m2 = as2.toMap
    m1.keySet.union(m2.keySet)
      .map(k => k -> (m1.get(k) ++ m2.get(k)).reduce(unifyAttribute _))
      .toMap
  }

  def concat(eb1: EdgeBundle, eb2: EdgeBundle): EdgeBundle = {
    val (weighted1, weighted2) = {
      val op = graph_operations.AddConstantDoubleEdgeAttribute(1.0)
      (op(op.edges, eb1).result.attr, op(op.edges, eb2).result.attr)
    }

    val op = graph_operations.ConcatenateBundles()
    op(op.weightsAB, weighted1)(op.weightsBC, weighted2).result.weightsAC.edgeBundle
  }
}
