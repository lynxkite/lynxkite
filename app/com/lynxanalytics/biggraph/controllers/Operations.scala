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
  import Operation.Category
  abstract class VertexOperation(p: Project)
    extends Operation(p, Category("Vertex operations", "blue"))
  abstract class EdgeOperation(p: Project)
    extends Operation(p, Category("Edge operations", "orange"))
  abstract class AttributeOperation(p: Project)
    extends Operation(p, Category("Attribute operations", "yellow"))
  abstract class CreateSegmentationOperation(p: Project)
    extends Operation(p, Category("Create segmentation", "green"))
  abstract class HiddenOperation(p: Project)
      extends Operation(p, Category("Hidden", "", visible = false)) {
    val description = ""
  }
  abstract class SegmentationOperation(p: Project)
      extends Operation(p, Category("Segmentation operations", "yellow", visible = p.isSegmentation)) {
    protected def seg = project.asSegmentation
    protected def parent = seg.parent
  }

  register(new VertexOperation(_) {
    val title = "Discard vertices"
    val description = ""
    val parameters = Seq()
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      project.vertexSet = null
    }
  })

  register(new EdgeOperation(_) {
    val title = "Discard edges"
    val description = ""
    val parameters = Seq()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      project.edgeBundle = null
    }
  })

  register(new VertexOperation(_) {
    val title = "New vertex set"
    val description = "Creates a new vertex set with no edges and no attributes."
    val parameters = Seq(
      Param("size", "Vertex set size"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val vs = graph_operations.CreateVertexSet(params("size").toInt)().result.vs
      project.vertexSet = vs
    }
  })

  register(new EdgeOperation(_) {
    val title = "Create random edge bundle"
    val description =
      """Creates edges randomly, so that each vertex will have a degree uniformly
      chosen between 0 and 2 × the provided parameter."""
    val parameters = Seq(
      Param("degree", "Average degree", defaultValue = "10"),
      Param("seed", "Seed", defaultValue = "0"))
    def enabled = hasVertexSet && hasNoEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FastRandomEdgeBundle(params("seed").toInt, params("degree").toInt)
      project.edgeBundle = op(op.vs, project.vertexSet).result.es
    }
  })

  register(new EdgeOperation(_) {
    val title = "Connect vertices on attribute"
    val description =
      "Creates edges between vertices that are equal in a chosen attribute."
    val parameters = Seq(
      Param("attr", "Attribute", options = vertexAttributes[String]))
    def enabled =
      (hasVertexSet && hasNoEdgeBundle
        && FEStatus.assert(vertexAttributes[String].nonEmpty, "No string vertex attributes."))
    private def applyOn[T](attr: VertexAttribute[T]) = {
      val op = graph_operations.EdgesFromAttributeMatches[T]()
      project.edgeBundle = op(op.attr, attr).result.edges
    }
    def apply(params: Map[String, String]) =
      applyOn(project.vertexAttributes(params("attr")))
  })

  val importHelpText =
    """ Wildcard (foo/*.csv) and glob (foo/{bar,baz}.csv) patterns are accepted. S3 paths must
      include the key name and secret key in the following format:
        <tt>s3n://key_name:secret_key@bucket/dir/file</tt>
      """

  register(new VertexOperation(_) {
    val title = "Import vertices"
    val description =
      "Imports vertices (no edges) from a CSV file, or files." + importHelpText
    val parameters = Seq(
      Param("files", "Files", kind = "file"),
      Param("header", "Header", defaultValue = "<read first line>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("filter", "(optional) Filtering expression"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val files = Filename(params("files"))
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
    }
  })

  register(new EdgeOperation(_) {
    val title = "Import edges for existing vertices"
    val description =
      """Imports edges from a CSV file, or files. Your vertices must have a key attribute, by which
      the edges can be attached to them.""" + importHelpText
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
      val files = Filename(params("files"))
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
    }
  })

  register(new EdgeOperation(_) {
    val title = "Import vertices and edges from single CSV fileset"
    val description =
      "Imports edges from a CSV file, or files." + importHelpText
    val parameters = Seq(
      Param("files", "Files", kind = "file"),
      Param("header", "Header", defaultValue = "<read first line>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"),
      Param("filter", "(optional) Filtering expression"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val files = Filename(params("files"))
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
    }
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Maximal cliques"
    val description = ""
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
    }
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Connected components"
    val description = ""
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
    }
  })

  register(new CreateSegmentationOperation(_) {
    val title = "Find infocom communities"
    val description = ""
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

      val weightedVertexToClique =
        graph_operations.AddConstantAttribute.run(cliquesResult.belongsTo.asVertexSet, 1.0)
      val weightedCliqueToCommunity =
        graph_operations.AddConstantAttribute.run(ccResult.belongsTo.asVertexSet, 1.0)

      val vertexToCommunity = {
        val op = graph_operations.ConcatenateBundles()
        op(
          op.edgesAB, cliquesResult.belongsTo)(
            op.edgesBC, ccResult.belongsTo)(
              op.weightsAB, weightedVertexToClique)(
                op.weightsBC, weightedCliqueToCommunity).result.edgesAC
      }

      val communitiesSegmentation = project.segmentation(params("communities_name"))
      communitiesSegmentation.project.vertexSet = ccResult.segments
      communitiesSegmentation.project.notes =
        "Infocom Communities of %s".format(project.projectName)
      communitiesSegmentation.belongsTo = vertexToCommunity
      computeSegmentSizes(communitiesSegmentation)
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add gaussian vertex attribute"
    val description =
      "Generates a new random double attribute with a Gaussian distribution."
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "random"),
      Param("seed", "Seed", defaultValue = "0"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val op = graph_operations.AddGaussianVertexAttribute(params("seed").toInt)
      project.vertexAttributes(params("name")) = op(op.vertices, project.vertexSet).result.attr
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add constant edge attribute"
    val description = ""
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"),
      Param("type", "Type", options = UIValue.seq(Seq("Double", "String"))))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val res = {
        if (params("type") == "Double") {
          val d = params("value").toDouble
          graph_operations.AddConstantAttribute.run(project.edgeBundle.asVertexSet, d)
        } else {
          graph_operations.AddConstantAttribute.run(project.edgeBundle.asVertexSet, params("value"))
        }
      }
      project.edgeAttributes(params("name")) = res
    }
  })

  register(new AttributeOperation(_) {
    val title = "Add constant vertex attribute"
    val description = ""
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"),
      Param("type", "Type", options = UIValue.seq(Seq("Double", "String"))))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val op: graph_operations.AddConstantAttribute[_] =
        graph_operations.AddConstantAttribute.doubleOrString(
          isDouble = (params("type") == "Double"), params("value"))
      project.vertexAttributes(params("name")) = op(op.vs, project.vertexSet).result.attr
    }
  })

  register(new AttributeOperation(_) {
    val title = "Pad with constant default value"
    val description =
      """An attribute may not be defined on every vertex. This operation sets a default value
      for the vertices where it was not defined."""
    val parameters = Seq(
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

  register(new EdgeOperation(_) {
    val title = "Reverse edge direction"
    val description = ""
    val parameters = Seq()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      project.edgeBundle = reverse(project.edgeBundle)
    }
  })

  register(new AttributeOperation(_) {
    val title = "Clustering coefficient"
    val description = ""
    val parameters = Seq(
      Param("name", "Attribute name", defaultValue = "clustering_coefficient"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ClusteringCoefficient()
      project.vertexAttributes(params("name")) = op(op.es, project.edgeBundle).result.clustering
    }
  })

  register(new AttributeOperation(_) {
    val title = "Degree"
    val description = ""
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
    }

    private def applyOn(es: EdgeBundle): VertexAttribute[Double] = {
      val op = graph_operations.OutDegree()
      op(op.es, es).result.outDegree
    }
  })

  register(new AttributeOperation(_) {
    val title = "PageRank"
    val description = ""
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
    }
  })

  register(new VertexOperation(_) {
    val title = "Example Graph"
    val description =
      "Creates small test graph with 4 people and 4 edges between them."
    val parameters = Seq()
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val g = graph_operations.ExampleGraph()().result
      project.vertexSet = g.vertices
      project.edgeBundle = g.edges
      project.vertexAttributes = g.vertexAttributes.mapValues(_.entity)
      project.edgeAttributes = g.edgeAttributes.mapValues(_.entity)
    }
  })

  register(new AttributeOperation(_) {
    val title = "Vertex attribute to string"
    val description = ""
    val parameters = Seq(
      Param("attr", "Vertex attribute", options = vertexAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
    private def applyOn[T](attr: VertexAttribute[T]) = {
      val op = graph_operations.VertexAttributeToString[T]()
      op(op.attr, attr).result.attr
    }
    def apply(params: Map[String, String]) = {
      for (attr <- params("attr").split(",")) {
        project.vertexAttributes(attr) = applyOn(project.vertexAttributes(attr))
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Vertex attribute to double"
    val description = ""
    val parameters = Seq(
      Param("attr", "Vertex attribute", options = vertexAttributes[String], multipleChoice = true))
    def enabled = FEStatus.assert(vertexAttributes[String].nonEmpty, "No string vertex attributes.")
    def apply(params: Map[String, String]) = {
      for (name <- params("attr").split(",")) {
        val attr = project.vertexAttributes(name).runtimeSafeCast[String]
        project.vertexAttributes(name) = toDouble(attr)
      }
    }
  })

  register(new VertexOperation(_) {
    val title = "Edge Graph"
    val description =
      """Creates the dual graph, where each vertex corresponds to an edge in the current graph.
      The vertices will be connected, if one corresponding edge is the continuation of the other.
      """
    val parameters = Seq()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.EdgeGraph()
      val g = op(op.es, project.edgeBundle).result
      project.vertexSet = g.newVS
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
    val parameters = Seq(
      Param("output", "Save as"),
      Param("expr", "Value", defaultValue = "1"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val expr = params("expr")
      var numAttrNames = List[String]()
      var numAttrs = List[VertexAttribute[Double]]()
      var strAttrNames = List[String]()
      var strAttrs = List[VertexAttribute[String]]()
      var vecAttrNames = List[String]()
      var vecAttrs = List[VertexAttribute[Vector[_]]]()
      project.vertexAttributes.foreach {
        case (name, attr) if expr.contains(name) && attr.is[Double] =>
          numAttrNames +:= name
          numAttrs +:= attr.runtimeSafeCast[Double]
        case (name, attr) if expr.contains(name) && attr.is[String] =>
          strAttrNames +:= name
          strAttrs +:= attr.runtimeSafeCast[String]
        case (name, attr) if expr.contains(name) && isVector(attr) =>
          implicit var tt = attr.typeTag
          vecAttrNames +:= name
          vecAttrs +:= vectorToAny(attr.asInstanceOf[VectorAttr[_]])
        case (name, attr) if expr.contains(name) =>
          log.warn(s"'$name' is of an unsupported type: ${attr.typeTag.tpe}")
        case _ => ()
      }
      val js = JavaScript(expr)
      // Figure out the return type.
      val op: graph_operations.DeriveJS[_] = testEvaluation(js, numAttrNames, strAttrNames, vecAttrNames) match {
        case _: String =>
          graph_operations.DeriveJSString(js, numAttrNames, strAttrNames, vecAttrNames)
        case _: Double =>
          graph_operations.DeriveJSDouble(js, numAttrNames, strAttrNames, vecAttrNames)
        case result =>
          throw new Exception(s"Test evaluation of '$js' returned '$result'.")
      }
      val result = op(
        op.vs, project.vertexSet)(
          op.numAttrs, numAttrs)(
            op.strAttrs, strAttrs)(
              op.vecAttrs, vecAttrs).result
      project.vertexAttributes(params("output")) = result.attr
    }

    def isVector[T](attr: VertexAttribute[T]): Boolean = {
      import scala.reflect.runtime.universe._
      // Vector is covariant, so Vector[X] <:< Vector[Any].
      return attr.typeTag.tpe <:< typeOf[Vector[Any]]
    }
    type VectorAttr[T] = VertexAttribute[Vector[T]]
    def vectorToAny[T](attr: VectorAttr[T]): VertexAttribute[Vector[Any]] = {
      val op = graph_operations.AttributeVectorToAny[T]()
      op(op.attr, attr).result.attr
    }

    // Evaluates the expression with 0/'' parameters.
    def testEvaluation(
      js: JavaScript,
      numAttrNames: Seq[String],
      strAttrNames: Seq[String],
      vecAttrNames: Seq[String]): Any = {
      val mapping = (
        numAttrNames.map(_ -> 0.0).toMap ++
        strAttrNames.map(_ -> "").toMap ++
        // Because the array will be empty for the test, the expression has to be ready
        // to handle this.
        vecAttrNames.map(_ -> Array[Any]()))
      return js.evaluate(mapping)
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
          attributeWithLocalAggregator(parent.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${attr}_${choice}") = result
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Weighted aggregate to segmentation"
    val description =
      "For example, it can calculate the average age per kilogram of each clique."
    def parameters = Seq(
      Param("weight", "Weight", options = UIValue.seq(parent.vertexAttributeNames[Double]))) ++
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
          attributeWithWeightedAggregator(weight, parent.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Aggregate from segmentation"
    val description =
      "For example, it can calculate the average size of cliques a person belongs to."
    def parameters = Seq(
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
          attributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        seg.parent.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register(new SegmentationOperation(_) {
    val title = "Weighted aggregate from segmentation"
    val description =
      """For example, it can calculate an averge over the cliques a person belongs to,
      weighted by the size of the cliques."""
    def parameters = Seq(
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
          attributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
        seg.parent.vertexAttributes(s"${prefix}${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate on neighbors"
    val description =
      "For example it can calculate the average age of the friends of each person."
    val parameters = Seq(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Param("direction", "Aggregate on",
        options = UIValue.seq(Seq("incoming edges", "outgoing edges")))) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes") && hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = params("direction") match {
        case "incoming edges" => project.edgeBundle
        case "outgoing edges" => reverse(project.edgeBundle)
      }
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          edges,
          attributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate on neighbors"
    val description =
      "For example it can calculate the average age per kilogram of the friends of each person."
    val parameters = Seq(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Param("weight", "Weight", options = vertexAttributes[Double]),
      Param("direction", "Aggregate on",
        options = UIValue.seq(Seq("incoming edges", "outgoing edges")))) ++
      aggregateParams(project.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes") && hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = params("direction") match {
        case "incoming edges" => project.edgeBundle
        case "outgoing edges" => reverse(project.edgeBundle)
      }
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((name, choice) <- parseAggregateParams(params)) {
        val attr = project.vertexAttributes(name)
        val result = aggregateViaConnection(
          edges,
          attributeWithWeightedAggregator(weight, attr, choice))
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
    val parameters = Seq(
      Param("key", "Match by", options = vertexAttributes)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def merge[T](attr: VertexAttribute[T]): graph_operations.MergeVertices.Output = {
      val op = graph_operations.MergeVertices[T]()
      op(op.attr, attr).result
    }
    def apply(params: Map[String, String]) = {
      val m = merge(project.vertexAttributes(params("key")))
      val oldVAttrs = project.vertexAttributes.toMap
      val oldEdges = project.edgeBundle
      val oldEAttrs = project.edgeAttributes.toMap
      project.vertexSet = m.segments
      // Always use most_common for the key attribute.
      val hack = "aggregate-" + params("key") -> "most_common"
      for ((attr, choice) <- parseAggregateParams(params + hack)) {
        val result = aggregateViaConnection(
          m.belongsTo,
          attributeWithLocalAggregator(oldVAttrs(attr), choice))
        project.vertexAttributes(attr) = result
      }
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
  })

  register(new EdgeOperation(_) {
    val title = "Merge parallel edges"
    val description = ""

    val parameters =
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) })

    def enabled = hasEdgeBundle

    def merge[T](attr: VertexAttribute[T]): graph_operations.MergeVertices.Output = {
      val op = graph_operations.MergeVertices[T]()
      op(op.attr, attr).result
    }
    def apply(params: Map[String, String]) = {
      val edgesAsAttr = {
        val op = graph_operations.EdgeBundleAsVertexAttribute()
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
            attributeWithLocalAggregator(oldAttrs(attrName), choice))
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate vertex attribute globally"
    val description = "The result is a single scalar value."
    val parameters = Seq(Param("prefix", "Generated name prefix", defaultValue = "")) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(attributeWithAggregator(project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate vertex attribute globally"
    val description = "The result is a single scalar value."
    val parameters = Seq(
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
          attributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}_by_${weightName}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate edge attribute globally"
    val description = "The result is a single scalar value."
    val parameters = Seq(Param("prefix", "Generated name prefix", defaultValue = "")) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) },
        needsGlobal = true)
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          attributeWithAggregator(project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate edge attribute globally"
    val description = "The result is a single scalar value."
    val parameters = Seq(
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
          attributeWithWeightedAggregator(weight, project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}_by_${weightName}"
        project.scalars(name) = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Aggregate edge attribute to vertices"
    val description =
      "For example it can calculate the average duration of calls for each person."
    val parameters = Seq(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Param("direction", "Aggregate on",
        options = UIValue.seq(Seq("incoming edges", "outgoing edges")))) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) })
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          project.edgeBundle,
          params("direction") == "outgoing edges",
          attributeWithLocalAggregator(
            project.edgeAttributes(attr),
            choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register(new AttributeOperation(_) {
    val title = "Weighted aggregate edge attribute to vertices"
    val description =
      "For example it can calculate the average cost per second of calls for each person."
    val parameters = Seq(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Param("weight", "Weight", options = edgeAttributes[Double]),
      Param("direction", "Aggregate on",
        options = UIValue.seq(Seq("incoming edges", "outgoing edges")))) ++
      aggregateParams(
        project.edgeAttributes.map { case (name, ea) => (name, ea) },
        weighted = true)
    def enabled =
      FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          project.edgeBundle,
          params("direction") == "outgoing edges",
          attributeWithWeightedAggregator(
            weight,
            project.edgeAttributes(attr),
            choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard edge attribute"
    val parameters = Seq(
      Param("name", "Name", options = edgeAttributes))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      project.edgeAttributes(params("name")) = null
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard vertex attribute"
    val parameters = Seq(
      Param("name", "Name", options = vertexAttributes))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      project.vertexAttributes(params("name")) = null
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard segmentation"
    val parameters = Seq(
      Param("name", "Name", options = segmentations))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]) = {
      project.segmentation(params("name")).remove
    }
  })

  register(new HiddenOperation(_) {
    val title = "Discard scalar"
    val parameters = Seq(
      Param("name", "Name", options = scalars))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    def apply(params: Map[String, String]) = {
      project.scalars(params("name")) = null
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename edge attribute"
    val parameters = Seq(
      Param("from", "Old name", options = edgeAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      project.edgeAttributes(params("to")) = project.edgeAttributes(params("from"))
      project.edgeAttributes(params("from")) = null
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename vertex attribute"
    val parameters = Seq(
      Param("from", "Old name", options = vertexAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      project.vertexAttributes(params("to")) = project.vertexAttributes(params("from"))
      project.vertexAttributes(params("from")) = null
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename segmentation"
    val parameters = Seq(
      Param("from", "Old name", options = segmentations),
      Param("to", "New name"))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    def apply(params: Map[String, String]) = {
      project.segmentation(params("from")).rename(params("to"))
    }
  })

  register(new VertexOperation(_) {
    val title = "Union with another project"
    val description =
      """The resulting graph is just a disconnected graph containing the vertices and edges of
      the two originating projects. All vertex and edge attributes are preserved. If an attribute
      exists in both projects, it must have the same data type in both.
      """
    val parameters = Seq(
      Param("other", "Other project's name", options = uIProjects))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]): Unit = {
      val other = Project(params("other"))
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
      project.edgeBundle = newEdgeBundle
      project.edgeAttributes = newEdgeAttributes
    }
  })

  register(new HiddenOperation(_) {
    val title = "Rename scalar"
    val parameters = Seq(
      Param("from", "Old name", options = scalars),
      Param("to", "New name"))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    def apply(params: Map[String, String]) = {
      project.scalars(params("to")) = project.scalars(params("from"))
      project.scalars(params("from")) = null
    }
  })

  register(new HiddenOperation(_) {
    val title = "Change project notes"
    val parameters = Seq(
      Param("notes", "New contents"))
    def enabled = FEStatus.enabled
    def apply(params: Map[String, String]) = {
      project.notes = params("notes")
    }
  })

  { // "Dirty operations", that is operations that use a data manager. Think twice if you really
    // need this before putting an operation here.
    implicit val dataManager = env.dataManager

    register(new AttributeOperation(_) {
      val title = "Export vertex attributes to CSV"
      val description = ""
      val parameters = Seq(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "vertex_attributes_csv"),
        Param("attrs", "Attributes", options = vertexAttributes, multipleChoice = true))
      def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "Nothing selected for export.")
        val labels = params("attrs").split(",")
        val attrs = labels.map(label => project.vertexAttributes(label))
        val path = getExportFilename(params("path"))
        val csv = graph_util.CSVExport.exportVertexAttributes(attrs, labels)
        csv.saveToDir(path)
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    def getExportFilename(param: String): Filename = {
      assert(param.nonEmpty, "No export path specified.")
      if (param == "<auto>") {
        dataManager.repositoryPath / "exports" / Timestamp.toString
      } else {
        Filename(param)
      }
    }

    register(new AttributeOperation(_) {
      val title = "Export edge attributes to CSV"
      val description = ""
      val parameters = Seq(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "edge_attributes_csv"),
        Param("attrs", "Attributes", options = edgeAttributes, multipleChoice = true))
      def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "Nothing selected for export.")
        val labels = params("attrs").split(",")
        val attrs = labels.map(label => project.edgeAttributes(label))
        val path = getExportFilename(params("path"))
        val csv = graph_util.CSVExport
          .exportEdgeAttributes(project.edgeBundle, attrs, labels)
        csv.saveToDir(path)
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    register(new SegmentationOperation(_) {
      val title = "Export segmentation to CSV"
      val description = ""
      val parameters = Seq(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "segmentation_csv"))
      def enabled = FEStatus.enabled
      def apply(params: Map[String, String]) = {
        val path = getExportFilename(params("path"))
        val csv = graph_util.CSVExport
          .exportEdgeAttributes(seg.belongsTo, Seq(), Seq())
        csv.saveToDir(path)
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })
  }

  def joinAttr[A, B](a: VertexAttribute[A], b: VertexAttribute[B]): VertexAttribute[(A, B)] = {
    val op = graph_operations.JoinAttributes[A, B]()
    op(op.a, a)(op.b, b).result.attr
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
    needsGlobal: Boolean = false,
    weighted: Boolean = false): Seq[FEOperationParameterMeta] = {
    attrs.toSeq.map {
      case (name, attr) =>
        val options = if (attr.is[Double]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.seq(Seq("ignore", "weighted_sum", "weighted_average", "by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.seq(Seq("ignore", "sum", "average", "min", "max", "count", "first"))
          } else {
            UIValue.seq(Seq("ignore", "sum", "average", "min", "max", "most_common", "count", "vector"))
          }
        } else if (attr.is[String]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.seq(Seq("ignore", "by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.seq(Seq("ignore", "count", "first"))
          } else {
            UIValue.seq(Seq("ignore", "most_common", "majority_50", "majority_100", "count", "vector"))
          }
        } else {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.seq(Seq("ignore", "by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.seq(Seq("ignore", "count", "first"))
          } else {
            UIValue.seq(Seq("ignore", "most_common", "count", "vector"))
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
  private def attributeWithWeightedAggregator[T](
    weight: VertexAttribute[Double], attr: VertexAttribute[T], choice: String): AttributeWithAggregator[_, _, _] = {
    choice match {
      case "by_max_weight" => AttributeWithAggregator(
        joinAttr(weight, attr), graph_operations.Aggregator.MaxBy[Double, T]())
      case "by_min_weight" => AttributeWithAggregator(
        joinAttr(graph_operations.DeriveJS.negative(weight), attr), graph_operations.Aggregator.MaxBy[Double, T]())
      case "weighted_sum" => AttributeWithAggregator(
        joinAttr(weight, attr.runtimeSafeCast[Double]), graph_operations.Aggregator.WeightedSum())
      case "weighted_average" => AttributeWithAggregator(
        joinAttr(weight, attr.runtimeSafeCast[Double]), graph_operations.Aggregator.WeightedAverage())
    }
  }

  private def attributeWithLocalAggregator[T](
    attr: VertexAttribute[T], choice: String): AttributeWithLocalAggregator[_, _] = {
    choice match {
      case "most_common" => AttributeWithLocalAggregator(attr, graph_operations.Aggregator.MostCommon[T]())
      case "majority_50" => AttributeWithLocalAggregator(attr.runtimeSafeCast[String], graph_operations.Aggregator.Majority(0.5))
      case "majority_100" => AttributeWithLocalAggregator(attr.runtimeSafeCast[String], graph_operations.Aggregator.Majority(1.0))
      case "vector" => AttributeWithLocalAggregator(attr, graph_operations.Aggregator.AsVector[T]())
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
    new graph_util.BundleChain(Seq(eb1, eb2)).getCompositeEdgeBundle._1
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
