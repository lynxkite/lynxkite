// Frontend operations for projects.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._

class Operations(env: SparkFreeEnvironment) extends OperationRepository(env) {
  val registries = Seq(
    new ImportOperations(env),
    new BuildGraphOperations(env),
    new SubgraphOperations(env),
    new BuildSegmentationOperations(env),
    new UseSegmentationOperations(env),
    new StructureOperations(env),
    new ScalarOperations(env),
    new VertexAttributeOperations(env),
    new EdgeAttributeOperations(env),
    new AttributePropagationOperations(env),
    new GraphComputationOperations(env),
    new MachineLearningOperations(env),
    new WorkflowOperations(env),
    new ManageProjectOperations(env),
    new ExportOperations(env),
    new VisualizationOperations(env),
    new HiddenOperations(env))

  override val atomicOperations = registries.flatMap(_.operations).toMap
  override val atomicCategories = registries.flatMap(_.categories).toMap
}

// The categories are collected here so that it is easier to manage them. E.g. sorting them or
// grouping them with colors.
object Categories {
  import com.lynxanalytics.biggraph.controllers.Operation.Category

  val ImportOperations = Category("Import", "green", icon = "glyphicon-folder-open", sortKey = "a")
  val BuildGraphOperations = Category("Build Graph", "blue", sortKey = "b")
  val SubgraphOperations = Category("Subgraph", "blue", sortKey = "c")
  val BuildSegmentationOperations = Category("Build segmentation", "blue", sortKey = "d")
  val UseSegmentationOperations = Category("Use segmentation", "blue", sortKey = "e")
  val StructureOperations = Category("Structure", "blue", sortKey = "f")
  val ScalarOperations = Category("Scalars", "blue", icon = "glyphicon-globe", sortKey = "g")
  val VertexAttributeOperations = Category("Vertex attributes", "blue", sortKey = "h")
  val EdgeAttributeOperations = Category("Edge attributes", "blue", sortKey = "i")
  val AttributePropagationOperations = Category("Attribute propagation", "blue", sortKey = "j")
  val GraphComputationOperations = Category("Graph computation", "blue", sortKey = "k")
  val MachineLearningOperations = Category("Machine learning", "blue", sortKey = "l")
  val WorkflowOperations = Category("Workflow", "blue", sortKey = "m")
  val ManageProjectOperations = Category("Manage project", "blue", sortKey = "n")
  val VisualizationOperations = Category("Visualization operations", "blue", sortKey = "o")
  val ExportOperations = Category("Export operations", "blue", icon = "glyphicon-folder-open", sortKey = "p")
  val HiddenOperations = Category("Manage project", "blue", visible = false, sortKey = "q")
}

class ProjectOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager

  protected val projectInput = "project" // The default input name, just to avoid typos.
  protected val projectOutput = "project"

  trait SegOp extends ProjectTransformation {
    protected def seg = project.asSegmentation
    protected def parent = seg.parent
    protected def addSegmentationParameters(): Unit
    if (project.isSegmentation) addSegmentationParameters()
  }

  import OperationParams._

  protected def segmentationSizesSquareSum(seg: SegmentationEditor, parent: ProjectEditor)(
    implicit manager: MetaGraphManager): Scalar[_] = {
    val size = aggregateViaConnection(
      seg.belongsTo,
      AttributeWithLocalAggregator(parent.vertexSet.idAttribute, "count")
    )
    val sizeSquare: Attribute[Double] = {
      val op = graph_operations.DeriveJSDouble(
        JavaScript("size * size"),
        Seq("size"))
      op(
        op.attrs,
        graph_operations.VertexAttributeToJSValue.seq(size)).result.attr
    }
    aggregate(AttributeWithAggregator(sizeSquare, "sum"))
  }

  protected def segmentationSizesProductSum(seg: SegmentationEditor, parent: ProjectEditor)(
    implicit manager: MetaGraphManager): Scalar[_] = {
    val size = aggregateViaConnection(
      seg.belongsTo,
      AttributeWithLocalAggregator(parent.vertexSet.idAttribute, "count")
    )
    val srcSize = graph_operations.VertexToEdgeAttribute.srcAttribute(size, seg.edgeBundle)
    val dstSize = graph_operations.VertexToEdgeAttribute.dstAttribute(size, seg.edgeBundle)
    val sizeProduct: Attribute[Double] = {
      val op = graph_operations.DeriveJSDouble(
        JavaScript("src_size * dst_size"),
        Seq("src_size", "dst_size"))
      op(
        op.attrs,
        graph_operations.VertexAttributeToJSValue.seq(srcSize, dstSize)).result.attr
    }
    aggregate(AttributeWithAggregator(sizeProduct, "sum"))
  }

  protected def getShapeFilePath(params: ParameterHolder): String = {
    val shapeFilePath = params("shapefile")
    assert(listShapefiles().exists(f => f.id == shapeFilePath),
      "Shapefile deleted, please choose another.")
    shapeFilePath
  }

  protected def listShapefiles(): List[FEOption] = {
    import java.io.File
    def metaDir = new File(env.metaGraphManager.repositoryPath).getParent
    val shapeDir = s"$metaDir/resources/shapefiles/"
    def lsR(f: File): Array[File] = {
      val files = f.listFiles()
      if (files == null)
        return Array.empty
      files.filter(_.getName.endsWith(".shp")) ++ files.filter(_.isDirectory).flatMap(lsR)
    }
    lsR(new File(shapeDir)).toList.map(f =>
      FEOption(f.getPath, f.getPath.substring(shapeDir.length)))
  }

  def computeSegmentSizes(segmentation: SegmentationEditor): Attribute[Double] = {
    val op = graph_operations.OutDegree()
    op(op.es, segmentation.belongsTo.reverse).result.outDegree
  }

  def toDouble(attr: Attribute[_]): Attribute[Double] = {
    if (attr.is[String])
      attr.runtimeSafeCast[String].asDouble
    else if (attr.is[Long])
      attr.runtimeSafeCast[Long].asDouble
    else if (attr.is[Int])
      attr.runtimeSafeCast[Int].asDouble
    else
      throw new AssertionError(s"Unexpected type (${attr.typeTag}) on $attr")
  }

  def parseAggregateParams(params: ParameterHolder) = {
    val aggregate = "aggregate_(.*)".r
    params.toMap.toSeq.collect {
      case (aggregate(attr), choices) if choices.nonEmpty => attr -> choices
    }.flatMap {
      case (attr, choices) => choices.split(",", -1).map(attr -> _)
    }
  }
  def aggregateParams(
    attrs: Iterable[(String, Attribute[_])],
    needsGlobal: Boolean = false,
    weighted: Boolean = false): List[OperationParameterMeta] = {
    val sortedAttrs = attrs.toList.sortBy(_._1)
    sortedAttrs.toList.map {
      case (name, attr) =>
        val options = if (attr.is[Double]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            FEOption.list("weighted_average", "by_max_weight", "by_min_weight", "weighted_sum")
          } else if (needsGlobal) {
            FEOption.list(
              "average", "count", "count_distinct", "count_most_common", "first", "max", "min", "most_common",
              "std_deviation", "sum")

          } else {
            FEOption.list(
              "average", "count", "count_distinct", "count_most_common", "first", "max", "median", "min", "most_common",
              "set", "std_deviation", "sum", "vector")
          }
        } else if (attr.is[String]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            FEOption.list("by_max_weight", "by_min_weight")
          } else if (needsGlobal) {
            FEOption.list("count", "count_distinct", "first", "most_common", "count_most_common")
          } else {
            FEOption.list(
              "count", "count_distinct", "first", "most_common", "count_most_common", "majority_50", "majority_100",
              "vector", "set")
          }
        } else {
          if (weighted) { // At the moment all weighted aggregators are global.
            FEOption.list("by_max_weight", "by_min_weight")
          } else if (needsGlobal) {
            FEOption.list("count", "count_distinct", "first", "most_common", "count_most_common")
          } else {
            FEOption.list("count", "count_distinct", "first", "median", "most_common", "count_most_common", "set", "vector")
          }
        }
        TagList(s"aggregate_$name", name, options = options)
    }
  }

  // Performs AggregateAttributeToScalar.
  protected def aggregate[From, Intermediate, To](
    attributeWithAggregator: AttributeWithAggregator[From, Intermediate, To]): Scalar[To] = {
    val op = graph_operations.AggregateAttributeToScalar(attributeWithAggregator.aggregator)
    op(op.attr, attributeWithAggregator.attr).result.aggregated
  }

  // Performs AggregateByEdgeBundle.
  protected def aggregateViaConnection[From, To](
    connection: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateByEdgeBundle(attributeWithAggregator.aggregator)
    op(op.connection, connection)(op.attr, attributeWithAggregator.attr).result.attr
  }
  private def mergeEdgesWithKey[T](edgesAsAttr: Attribute[(ID, ID)], keyAttr: Attribute[T]) = {
    val edgesAndKey: Attribute[((ID, ID), T)] = edgesAsAttr.join(keyAttr)
    val op = graph_operations.MergeVertices[((ID, ID), T)]()
    op(op.attr, edgesAndKey).result
  }

  protected def mergeEdges(edgesAsAttr: Attribute[(ID, ID)]) = {
    val op = graph_operations.MergeVertices[(ID, ID)]()
    op(op.attr, edgesAsAttr).result
  }

  // Common code for operations "merge parallel edges" and "merge parallel edges by key"
  protected def applyMergeParallelEdges(
    project: ProjectEditor, params: ParameterHolder, byKey: Boolean) = {

    val edgesAsAttr = {
      val op = graph_operations.EdgeBundleAsAttribute()
      op(op.edges, project.edgeBundle).result.attr
    }

    val mergedResult =
      if (byKey) {
        val keyAttr = project.edgeAttributes(params("key"))
        mergeEdgesWithKey(edgesAsAttr, keyAttr)
      } else {
        mergeEdges(edgesAsAttr)
      }

    val newEdges = {
      val op = graph_operations.PulledOverEdges()
      op(op.originalEB, project.edgeBundle)(op.injection, mergedResult.representative)
        .result.pulledEB
    }
    val oldAttrs = project.edgeAttributes.toMap
    project.edgeBundle = newEdges

    for ((attr, choice) <- parseAggregateParams(params)) {
      project.edgeAttributes(s"${attr}_${choice}") =
        aggregateViaConnection(
          mergedResult.belongsTo,
          AttributeWithLocalAggregator(oldAttrs(attr), choice))
    }
    if (byKey) {
      val key = params("key")
      project.edgeAttributes(key) =
        aggregateViaConnection(mergedResult.belongsTo,
          AttributeWithLocalAggregator(oldAttrs(key), "most_common"))
    }
  }

  // Performs AggregateFromEdges.
  protected def aggregateFromEdges[From, To](
    edges: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateFromEdges(attributeWithAggregator.aggregator)
    val res = op(op.edges, edges)(op.eattr, attributeWithAggregator.attr).result
    res.dstAttr
  }

  def stripDuplicateEdges(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.StripDuplicateEdgesFromBundle()
    op(op.es, eb).result.unique
  }

  object Direction {
    // Options suitable when edge attributes are involved.
    val attrOptions = FEOption.list("incoming edges", "outgoing edges", "all edges")
    def attrOptionsWithDefault(default: String): List[FEOption] = {
      assert(attrOptions.map(_.id).contains(default), s"$default not in $attrOptions")
      FEOption.list(default) ++ attrOptions.filter(_.id != default)
    }
    // Options suitable when only neighbors are involved.
    val neighborOptions = FEOption.list(
      "in-neighbors", "out-neighbors", "all neighbors", "symmetric neighbors")
    // Options suitable when edge attributes are not involved.
    val options = attrOptions ++ FEOption.list("symmetric edges") ++ neighborOptions
    // Neighborhood directions correspond to these
    // edge directions, but they also retain only one A->B edge in
    // the output edgeBundle
    private val neighborOptionMapping = Map(
      "in-neighbors" -> "incoming edges",
      "out-neighbors" -> "outgoing edges",
      "all neighbors" -> "all edges",
      "symmetric neighbors" -> "symmetric edges"
    )
  }
  case class Direction(direction: String, origEB: EdgeBundle, reversed: Boolean = false) {
    val unchangedOut: (EdgeBundle, Option[EdgeBundle]) = (origEB, None)
    val reversedOut: (EdgeBundle, Option[EdgeBundle]) = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, origEB).result
      (res.esBA, Some(res.injection))
    }
    private def computeEdgeBundleAndPullBundleOpt(dir: String): (EdgeBundle, Option[EdgeBundle]) = {
      dir match {
        case "incoming edges" => if (reversed) reversedOut else unchangedOut
        case "outgoing edges" => if (reversed) unchangedOut else reversedOut
        case "all edges" =>
          val op = graph_operations.AddReversedEdges()
          val res = op(op.es, origEB).result
          (res.esPlus, Some(res.newToOriginal))
        case "symmetric edges" =>
          // Use "null" as the injection because it is an error to use
          // "symmetric edges" with edge attributes.
          (origEB.makeSymmetric, Some(null))
      }
    }

    val (edgeBundle, pullBundleOpt): (EdgeBundle, Option[EdgeBundle]) = {
      if (Direction.neighborOptionMapping.contains(direction)) {
        val (eB, pBO) = computeEdgeBundleAndPullBundleOpt(Direction.neighborOptionMapping(direction))
        (stripDuplicateEdges(eB), pBO)
      } else {
        computeEdgeBundleAndPullBundleOpt(direction)
      }
    }

    def pull[T](attribute: Attribute[T]): Attribute[T] = {
      pullBundleOpt.map(attribute.pullVia(_)).getOrElse(attribute)
    }
  }

  protected def unifyAttributeT[T](a1: Attribute[T], a2: Attribute[_]): Attribute[T] = {
    a1.fallback(a2.runtimeSafeCast(a1.typeTag))
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

  def newScalar(data: String): Scalar[String] = {
    val op = graph_operations.CreateStringScalar(data)
    op.result.created
  }
}

object JSUtilities {
  // Listing the valid characters for JS variable names. The \\p{*} syntax is for specifying
  // Unicode categories for scala regex.
  // For more information about the valid variable names in JS please consult:
  // http://es5.github.io/x7.html#x7.6
  val validJSCharacters = "_$\\p{Lu}\\p{Ll}\\p{Lt}\\p{Lm}\\p{Lo}\\p{Nl}\\p{Mn}" +
    "\\p{Mc}\\p{Nd}\\p{Pc}\\u200C\\u200D\\\\"
  val validJSFirstCharacters = "_$\\p{Lu}\\p{Ll}\\p{Lt}\\p{Lm}\\p{Lo}\\p{Nl}\\\\"

  def collectIdentifiers[T <: MetaGraphEntity](
    holder: StateMapHolder[T],
    expr: String,
    prefix: String = ""): IndexedSeq[(String, T)] = {
    holder.filter {
      case (name, _) => containsIdentifierJS(expr, prefix + name)
    }.toIndexedSeq
  }

  // Whether a string can be a JavaScript identifier.
  def canBeValidJSIdentifier(identifier: String): Boolean = {
    val re = s"^[${validJSFirstCharacters}][${validJSCharacters}]*$$"
    identifier.matches(re)
  }

  // Whether a JavaScript expression contains a given identifier.
  // It's a best-effort implementation with no guarantees of correctness.
  def containsIdentifierJS(expr: String, identifier: String): Boolean = {
    if (!canBeValidJSIdentifier(identifier)) {
      false
    } else {
      val quotedIdentifer = java.util.regex.Pattern.quote(identifier)
      val re = s"(?s)(^|.*[^$validJSCharacters])${quotedIdentifer}($$|[^$validJSCharacters].*)"
      expr.matches(re)
    }
  }
}
