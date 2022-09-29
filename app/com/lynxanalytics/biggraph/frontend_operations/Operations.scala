// Frontend operations for projects.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_util.Environment

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
    new SegmentationAttributeOperations(env),
    new AttributePropagationOperations(env),
    new GraphComputationOperations(env),
    new MachineLearningOperations(env),
    new WorkflowOperations(env),
    new ManageProjectOperations(env),
    new ExportOperations(env),
    new VisualizationOperations(env),
    new HiddenOperations(env),
  )

  override val atomicOperations = registries.flatMap(_.operations).toMap
  override val atomicCategories = registries.flatMap(_.categories).toMap
}

// The categories are collected here so that it is easier to manage them. E.g. sorting them or
// grouping them with colors.
object Categories {
  import com.lynxanalytics.biggraph.controllers.Operation.Category

  // Assign indices in declaration order.
  var lastIdx = 0
  def idx = { lastIdx += 1; lastIdx }

  val ImportOperations = Category("Import", "green", icon = "upload", index = idx)
  val BuildGraphOperations = Category("Build graph", "green", icon = "gavel", index = idx)
  val SubgraphOperations = Category("Subgraph", "green", icon = "filter", index = idx)
  val BuildSegmentationOperations =
    Category("Build segmentation", "green", icon = "th-large", index = idx)
  val UseSegmentationOperations =
    Category("Use segmentation", "blue", icon = "th-large", index = idx)
  val StructureOperations =
    Category("Structure", "orange", icon = "asterisk", index = idx)
  val ScalarOperations =
    Category("Graph attributes", "orange", icon = "globe-americas", index = idx)
  val VertexAttributeOperations =
    Category("Vertex attributes", "orange", icon = "dot-circle", index = idx)
  val EdgeAttributeOperations =
    Category("Edge attributes", "orange", icon = "share-alt", index = idx)
  val SegmentationAttributeOperations =
    Category("Segmentation attributes", "orange", icon = "th-large", index = idx)
  val AttributePropagationOperations =
    Category("Attribute propagation", "orange", icon = "podcast", index = idx)
  val GraphComputationOperations =
    Category("Graph computation", "blue", icon = "snowflake", index = idx)
  val MachineLearningOperations =
    Category("Machine learning", "blue", icon = "robot", index = idx)
  val WorkflowOperations =
    Category("Workflow", "blue", icon = "cogs", index = idx)
  val ManageProjectOperations =
    Category("Manage graph", "blue", icon = "wrench", index = idx)
  val VisualizationOperations =
    Category("Visualization operations", "purple", icon = "eye", index = idx)
  val ExportOperations =
    Category("Export operations", "purple", icon = "download", index = idx)
  val HiddenOperations =
    Category("Hidden operations", "orange", icon = "ankh", visible = false, index = idx)
}

abstract class ProjectOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  import Operation.Context

  val category: Operation.Category
  override def defaultIcon = category.icon

  implicit lazy val manager = env.metaGraphManager

  protected val projectInput = "graph" // The default input name, just to avoid typos.
  protected val projectOutput = "graph"

  def registerProjectCreatingOp(id: String)(factory: Context => ProjectOutputOperation): Unit = {
    registerOp(id, defaultIcon, category, List(), List(projectOutput), factory)
  }

  def register(id: String)(factory: Context => ProjectTransformation): Unit = {
    registerOp(id, defaultIcon, category, List(projectInput), List(projectOutput), factory)
  }

  def register(id: String, inputs: List[String])(factory: Context => ProjectOutputOperation): Unit = {
    registerOp(id, defaultIcon, category, inputs, List(projectOutput), factory)
  }

  def register(id: String, inputs: List[String], outputs: List[String], icon: String = defaultIcon)(
      factory: Context => Operation): Unit = {
    registerOp(id, icon, category, inputs, outputs, factory)
  }

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
      AttributeWithLocalAggregator(parent.vertexSet.idAttribute, "count"))
    val sizeSquare = graph_operations.DeriveScala.derive[Double](
      "size * size",
      Seq("size" -> size))
    aggregate(AttributeWithAggregator(sizeSquare, "sum"))
  }

  protected def segmentationSizesProductSum(seg: SegmentationEditor, parent: ProjectEditor)(
      implicit manager: MetaGraphManager): Scalar[_] = {
    val size = aggregateViaConnection(
      seg.belongsTo,
      AttributeWithLocalAggregator(parent.vertexSet.idAttribute, "count"))
    val srcSize = graph_operations.VertexToEdgeAttribute.srcAttribute(size, seg.edgeBundle)
    val dstSize = graph_operations.VertexToEdgeAttribute.dstAttribute(size, seg.edgeBundle)
    val sizeProduct = graph_operations.DeriveScala.derive[Double](
      "src_size * dst_size",
      Seq("src_size" -> srcSize, "dst_size" -> dstSize))
    aggregate(AttributeWithAggregator(sizeProduct, "sum"))
  }

  protected def getShapeFilePath(params: ParameterHolder): String = {
    val shapeFilePath = params("shapefile")
    assert(
      listShapefiles().exists(f => f.id == shapeFilePath),
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
    else if (attr.is[Double])
      attr.runtimeSafeCast[Double]
    else
      throw new AssertionError(s"Unexpected type (${attr.typeTag}) on $attr")
  }

  def parseAggregateParams(params: ParameterHolder, weight: String = null) = {
    val extraSuffix = if (weight == null) "" else s"_by_$weight"
    val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
    val addSuffix = params("add_suffix") == "yes"
    val aggregate = "aggregate_(.*)".r
    params.toMap.toSeq.collect {
      case (aggregate(attr), choices) if choices.nonEmpty => attr -> choices
    }.flatMap {
      case (attr, choices) =>
        assert(
          addSuffix || !choices.contains(","),
          s"Suffixes are necessary when multiple aggregations are configured for $attr.")
        choices.split(",", -1).map {
          c => (attr, c, if (addSuffix) s"$prefix${attr}_$c$extraSuffix" else s"$prefix$attr")
        }
    }
  }
  def aggregateParams(
      attrs: Iterable[(String, Attribute[_])],
      needsGlobal: Boolean = false,
      weighted: Boolean = false,
      defaultPrefix: String = ""): List[OperationParameterMeta] = {
    val sortedAttrs = attrs.toList.sortBy(_._1)
    Param("prefix", "Generated name prefix", defaultValue = defaultPrefix) ::
      Choice("add_suffix", "Add suffixes to attribute names", options = FEOption.yesno) ::
      sortedAttrs.toList.map {
        case (name, attr) =>
          val options =
            if (attr.is[Double]) {
              if (weighted) { // At the moment all weighted aggregators are global.
                FEOption.list("weighted_average", "by_max_weight", "by_min_weight", "weighted_sum")
              } else if (needsGlobal) {
                FEOption.list(
                  "average",
                  "count",
                  "count_distinct",
                  "count_most_common",
                  "first",
                  "max",
                  "min",
                  "most_common",
                  "std_deviation",
                  "sum")

              } else {
                FEOption.list(
                  "average",
                  "count",
                  "count_distinct",
                  "count_most_common",
                  "first",
                  "max",
                  "median",
                  "min",
                  "most_common",
                  "set",
                  "std_deviation",
                  "sum",
                  "vector")
              }
            } else if (attr.is[String]) {
              if (weighted) { // At the moment all weighted aggregators are global.
                FEOption.list("by_max_weight", "by_min_weight")
              } else if (needsGlobal) {
                FEOption.list("count", "count_distinct", "first", "most_common", "count_most_common")
              } else {
                FEOption.list(
                  "count",
                  "count_distinct",
                  "first",
                  "most_common",
                  "count_most_common",
                  "majority_50",
                  "majority_100",
                  "vector",
                  "set")
              }
            } else if (attr.is[Vector[Double]]) {
              if (weighted) {
                FEOption.list("by_max_weight", "by_min_weight")
              } else {
                FEOption.list(
                  "concatenate",
                  "count",
                  "count_distinct",
                  "count_most_common",
                  "elementwise_average",
                  "elementwise_max",
                  "elementwise_min",
                  "elementwise_std_deviation",
                  "elementwise_sum",
                  "first",
                  "most_common",
                )
              }
            } else {
              if (weighted) { // At the moment all weighted aggregators are global.
                FEOption.list("by_max_weight", "by_min_weight")
              } else if (needsGlobal) {
                FEOption.list("count", "count_distinct", "first", "most_common", "count_most_common")
              } else {
                FEOption.list(
                  "count",
                  "count_distinct",
                  "first",
                  "median",
                  "most_common",
                  "count_most_common",
                  "set",
                  "vector")
              }
            }
          TagList(s"aggregate_$name", name, options = options)
      }
  }

  // Aggregation parameters which are empty - i.e. no aggregator was defined - should be removed.
  protected def cleanAggregateParams(params: Map[String, String]): Map[String, String] = {
    params.filter { case (k, v) => !k.startsWith("aggregate_") || v.nonEmpty }
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
    op(
      op.connectionBySrc,
      graph_operations.HybridEdgeBundle.bySrc(connection))(
      op.attr,
      attributeWithAggregator.attr).result.attr
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
      project: ProjectEditor,
      params: ParameterHolder,
      byKey: Boolean) = {

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

    for ((attr, choice, name) <- parseAggregateParams(params)) {
      project.edgeAttributes(name) =
        aggregateViaConnection(
          mergedResult.belongsTo,
          AttributeWithLocalAggregator(oldAttrs(attr), choice))
    }
    if (byKey) {
      val key = params("key")
      project.edgeAttributes(key) =
        aggregateViaConnection(
          mergedResult.belongsTo,
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
    val attrOptions = FEOption.list("all edges", "incoming edges", "outgoing edges")
    def attrOptionsWithDefault(default: String): List[FEOption] = {
      assert(attrOptions.map(_.id).contains(default), s"$default not in $attrOptions")
      FEOption.list(default) ++ attrOptions.filter(_.id != default)
    }
    // Options suitable when only neighbors are involved.
    val neighborOptions = FEOption.list(
      "in-neighbors",
      "out-neighbors",
      "all neighbors",
      "symmetric neighbors")
    // Options suitable when edge attributes are not involved.
    val options = attrOptions ++ FEOption.list("symmetric edges") ++ neighborOptions
    // Neighborhood directions correspond to these
    // edge directions, but they also retain only one A->B edge in
    // the output edgeBundle
    private val neighborOptionMapping = Map(
      "in-neighbors" -> "incoming edges",
      "out-neighbors" -> "outgoing edges",
      "all neighbors" -> "all edges",
      "symmetric neighbors" -> "symmetric edges")
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

  def checkTypeCollision(project: ProjectEditor, other: ProjectViewer) = {
    val commonAttributeNames =
      project.vertexAttributes.keySet & other.vertexAttributes.keySet
    for (name <- commonAttributeNames) {
      val a1 = project.vertexAttributes(name)
      val a2 = other.vertexAttributes(name)
      assert(
        a1.typeTag.tpe =:= a2.typeTag.tpe,
        s"Attribute '$name' has conflicting types in the two projects: " +
          s"(${a1.typeTag.tpe} and ${a2.typeTag.tpe})")
    }
  }

  def mergeInto(project: ProjectEditor, other: ProjectViewer): Unit = {
    if (project.vertexSet == null) {
      project.vertexSet = other.vertexSet
      project.vertexAttributes = other.vertexAttributes
      project.edgeBundle = other.edgeBundle
      project.edgeAttributes = other.edgeAttributes
      return
    }

    checkTypeCollision(project, other)
    val vsUnion = {
      val op = graph_operations.VertexSetUnion(2)
      op(op.vss, Seq(project.vertexSet, other.vertexSet)).result
    }

    val newVertexAttributes = unifyAttributes(
      project.vertexAttributes
        .map {
          case (name, attr) =>
            name -> attr.pullVia(vsUnion.injections(0).reverse)
        },
      other.vertexAttributes
        .map {
          case (name, attr) =>
            name -> attr.pullVia(vsUnion.injections(1).reverse)
        },
    )
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
            op.ebs,
            Seq(ebInduced.get.induced.entity, otherEbInduced.get.induced.entity))(
            op.injections,
            idUnion.injections.map(_.entity)).result.union
        }
        (
          ebUnion,
          idUnion.injections(0).reverse.concat(ebInduced.get.embedding),
          idUnion.injections(1).reverse.concat(otherEbInduced.get.embedding))
      } else {
        (null, null, null)
      }
    val newEdgeAttributes = unifyAttributes(
      project.edgeAttributes
        .map {
          case (name, attr) => name -> attr.pullVia(myEbInjection)
        },
      other.edgeAttributes
        .map {
          case (name, attr) => name -> attr.pullVia(otherEbInjection)
        },
    )

    project.vertexSet = vsUnion.union
    for ((name, attr) <- newVertexAttributes) {
      project.newVertexAttribute(name, attr) // Clear notes.
    }
    project.edgeBundle = newEdgeBundle
    project.edgeAttributes = newEdgeAttributes
  }
}

object ScalaUtilities {
  import com.lynxanalytics.sandbox.ScalaScript

  def collectIdentifiers[T <: MetaGraphEntity](
      holder: StateMapHolder[T],
      expr: String,
      prefix: String = ""): IndexedSeq[(String, T)] = {
    var vars = ScalaScript.findVariables(expr)
    holder.filter {
      case (name, _) => vars.contains(prefix + name)
    }.toIndexedSeq
  }
}

object PythonUtilities {
  import graph_operations.DerivePython._

  val allowed = Environment.envOrElse("KITE_ALLOW_PYTHON", "") match {
    case "yes" => true
    case "no" => false
    case "" => false
    case unexpected => throw new AssertionError(
        s"KITE_ALLOW_PYTHON must be either 'yes' or 'no'. Found '$unexpected'.")
  }
  def assertAllowed() = {
    assert(allowed, "Python code execution is disabled on this server for security reasons.")
  }

  private def toSerializableType(pythonType: String) = {
    pythonType match {
      case "str" => SerializableType.string
      case "float" => SerializableType.double
      case "int" => SerializableType.long
      case "np.ndarray" => SerializableType.vector(SerializableType.double)
      case _ => throw new AssertionError(s"Unknown type: $pythonType")
    }
  }

  // Parses the output list into Fields.
  def outputFields(outputs: Seq[String], api: Seq[String]): Seq[Field] = {
    val outputDeclaration = raw"(\w+)\.(\w+)\s*:\s*([a-zA-Z0-9.]+)".r
    outputs.map {
      case outputDeclaration(parent, name, tpe) =>
        assert(
          api.contains(parent),
          s"Invalid output: '$parent.$name'. Valid groups are: " + api.mkString(", "))
        Field(parent, name, toSerializableType(tpe))
      case output => throw new AssertionError(
          s"Output declarations must be formatted like 'vs.my_attr: str'. Got '$output'.")
    }
  }

  def derive(
      code: String,
      inputs: Seq[String],
      outputs: Seq[String],
      project: com.lynxanalytics.biggraph.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Unit = {
    val api = Seq("vs", "es", "graph_attributes")
    // Parse the input list into Fields.
    val existingFields: Map[String, () => Field] = project.vertexAttributes.map {
      case (name, attr) => s"vs.$name" -> (() => Field("vs", name, SerializableType(attr.typeTag)))
    }.toMap ++ project.edgeAttributes.map {
      case (name, attr) => s"es.$name" -> (() => Field("es", name, SerializableType(attr.typeTag)))
    }.toMap ++ project.scalars.map {
      case (name, s) =>
        s"graph_attributes.$name" -> (() => Field("graph_attributes", name, SerializableType(s.typeTag)))
    }.toMap + {
      "es.src" -> (() => Field("es", "src", SerializableType.long))
    } + {
      "es.dst" -> (() => Field("es", "dst", SerializableType.long))
    }
    val inputFields: Seq[Field] = inputs.map { i =>
      existingFields.get(i) match {
        case Some(f) => f()
        case None => throw new AssertionError(
            s"No available input called '$i'. Available inputs are: " +
              existingFields.keys.toSeq.sorted.mkString(", "))
      }
    }
    // Run the operation.
    val op = graph_operations.DerivePython(code, inputFields.toList, outputFields(outputs, api).toList)
    import Scripting._
    val builder = InstanceBuilder(op)
    for ((f, i) <- op.attrFields.zipWithIndex) {
      val attr = f.parent match {
        case "vs" => project.vertexAttributes(f.name)
        case "es" => project.edgeAttributes(f.name)
      }
      builder(op.attrs(i), attr)
    }
    for (f <- op.edgeParents) {
      builder(op.ebs(f), project.edgeBundle)
    }
    for ((f, i) <- op.scalarFields.zipWithIndex) {
      builder(op.scalars(i), project.scalars(f.name))
    }
    builder.toInstance(manager)
    val res = builder.result
    // Save the outputs into the project.
    for ((f, i) <- res.attrFields.zipWithIndex) {
      f.parent match {
        case "vs" => project.newVertexAttribute(f.name, res.attrs(i))
        case "es" => project.newEdgeAttribute(f.name, res.attrs(i))
      }
    }
    for ((f, i) <- res.scalarFields.zipWithIndex) {
      project.newScalar(f.name, res.scalars(i))
    }
  }

  def create(
      code: String,
      outputs: Seq[String],
      project: com.lynxanalytics.biggraph.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Unit = {
    val api = Seq("vs", "es", "graph_attributes")
    // Run the operation.
    val res = graph_operations.CreateGraphInPython(code, outputFields(outputs, api).toList)().result
    project.vertexSet = res.vertices
    project.edgeBundle = res.edges
    // Save the outputs into the project.
    for ((f, i) <- res.attrFields.zipWithIndex) {
      f.parent match {
        case "vs" => project.newVertexAttribute(f.name, res.attrs(i))
        case "es" => project.newEdgeAttribute(f.name, res.attrs(i))
      }
    }
    for ((f, i) <- res.scalarFields.zipWithIndex) {
      project.newScalar(f.name, res.scalars(i))
    }
  }

  def inferInputs(code: String, kind: String): Seq[String] = {
    val api = kind match {
      case BoxOutputKind.Project => Seq("vs", "es", "graph_attributes")
      case BoxOutputKind.Table => Seq("df")
    }
    val outputs = inferOutputs(code, kind).map(_.replaceFirst(":.*", "")).toSet
    val mentions = api.flatMap { parent =>
      val a = s"\\b$parent\\.\\w+".r.findAllMatchIn(code).map(_.matched).toSeq
      val b = s"""\\b$parent\\s*\\[\\s*['"](\\w+)['"]\\s*\\]""".r
        .findAllMatchIn(code).map(m => s"$parent.${m.group(1)}").toSeq
      a ++ b
    }.toSet
    (mentions -- outputs).toSeq.sorted
  }
  def inferOutputs(code: String, kind: String): Seq[String] = {
    val api = kind match {
      case BoxOutputKind.Project => Seq("vs", "es", "graph_attributes")
      case BoxOutputKind.Table => Seq("df")
    }
    api.flatMap { parent =>
      val a = s"""\\b$parent\\.\\w+\\s*:\\s*[a-zA-Z0-9.]+""".r
        .findAllMatchIn(code).map(_.matched).toSeq
      val b = s"""\\b$parent\\s*\\[\\s*['"](\\w+)['"]\\s*\\]\\s*:\\s*([a-zA-Z0-9.]+)""".r
        .findAllMatchIn(code).map(m => s"$parent.${m.group(1)}: ${m.group(2)}").toSeq
      a ++ b
    }.sorted
  }

  def deriveTable(
      code: String,
      table: Table,
      outputs: Seq[String])(
      implicit manager: MetaGraphManager): Table = {
    val api = Seq("df")
    // Run the operation.
    val op = graph_operations.DeriveTablePython(code, outputFields(outputs, api).toList)
    op(op.df, table).result.df
  }
}
