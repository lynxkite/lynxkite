package com.lynxanalytics.biggraph.controllers

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_api.Scripting._

case class FEVertexAttributeFilter(
  val attributeId: String,
  val valueSpec: String)
case class VertexDiagramSpec(
  val vertexSetId: String,
  val filters: Seq[FEVertexAttributeFilter],
  val mode: String, // For now, one of "bucketed", "sampled".

  // ** Parameters for bucketed view **
  // Empty string means no bucketing on that axis.
  val xBucketingAttributeId: String = "",
  val xNumBuckets: Int = 1,
  val yBucketingAttributeId: String = "",
  val yNumBuckets: Int = 1,

  // ** Parameters for sampled view **
  // Empty string means auto select randomly.
  val centralVertexId: String = "",
  // Edge bundle used to find neighborhood of the central vertex.
  val sampleSmearEdgeBundleId: String = "",
  val sizeAttributeId: String = "",
  val labelAttributeId: String = "",
  val radius: Int = 1)

case class FEVertex(
  size: Double,

  // For bucketed view:
  x: Int = 0,
  y: Int = 0,

  // For sampled view:
  id: Long = 0,
  label: String = "")

case class VertexDiagramResponse(
  val diagramId: String,
  val vertices: Seq[FEVertex],
  val mode: String, // as specified in the request

  // ** Only set for bucketed view **
  val xLabelType: String = "",
  val yLabelType: String = "",
  val xLabels: Seq[String] = Seq(),
  val yLabels: Seq[String] = Seq(),

  // ** Only set for sampled view **
  val center: ID = 0)

case class EdgeDiagramSpec(
  // In the context of an FEGraphRequest "idx[4]" means the diagram requested by vertexSets(4).
  // Otherwise a UUID obtained by a previous vertex diagram request.
  val srcDiagramId: String,
  val dstDiagramId: String,
  // These are copied verbatim to the response, used by the FE to identify EdgeDiagrams.
  val srcIdx: Int,
  val dstIdx: Int,
  val bundleSequence: Seq[BundleSequenceStep])

case class BundleSequenceStep(bundle: String, reversed: Boolean)

case class FEEdge(
  // idx of source vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  a: Int,
  // idx of destination vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  b: Int,
  size: Int)

case class EdgeDiagramResponse(
  val srcDiagramId: String,
  val dstDiagramId: String,

  // Copied from the request.
  val srcIdx: Int,
  val dstIdx: Int,

  val edges: Seq[FEEdge])

case class FEGraphRequest(
  vertexSets: Seq[VertexDiagramSpec],
  edgeBundles: Seq[EdgeDiagramSpec])

case class FEGraphRespone(
  vertexSets: Seq[VertexDiagramResponse],
  edgeBundles: Seq[EdgeDiagramResponse])

class GraphDrawingController(env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager

  import graph_operations.SampledVertexAttribute.sampleAttribute

  def applyFEFilter(filterSpec: FEVertexAttributeFilter): VertexSet = {
    val attr = metaManager.vertexAttribute(filterSpec.attributeId.asUUID)
    attr.rdd.cache()
    FEFilters.filteredBaseSet(
      metaManager,
      attr,
      filterSpec.valueSpec)
  }

  def intersect(filteredVss: Seq[VertexSet]): VertexSet = {
    val op = graph_operations.VertexSetIntersection(filteredVss.size)
    val builder = filteredVss.zipWithIndex.foldLeft(op.builder) {
      case (b, (vs, i)) => b(op.vss(i), vs)
    }
    builder.result.intersection
  }

  def filter(vertexSet: VertexSet, filters: Seq[FEVertexAttributeFilter]): VertexSet = {
    if (filters.isEmpty) return vertexSet
    intersect(filters.map(applyFEFilter))
  }

  def filterMore(filtered: VertexSet, moreFilters: Seq[FEVertexAttributeFilter]): VertexSet = {
    intersect(filtered +: moreFilters.map(applyFEFilter))
  }

  def getVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    request.mode match {
      case "bucketed" => getBucketedVertexDiagram(request)
      case "sampled" => getSampledVertexDiagram(request)
    }
  }

  /*def getSampledVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    val countOp = graph_operations.CountVertices()
    val count = countOp(countOp.vertices, vertexSet).result.count.value
    val filtered = filter(vertexSet, request.filters)
    val op = graph_operations.SampledView(
      request.centralVertexId,
      request.radius,
      request.sampleSmearEdgeBundleId.nonEmpty,
      request.sizeAttributeId.nonEmpty,
      request.labelAttributeId.nonEmpty)
    var inputs = MetaDataSet(Map('vertices -> filtered))
    if (request.sampleSmearEdgeBundleId.nonEmpty) {
      inputs ++= MetaDataSet(Map('edges -> metaManager.edgeBundle(request.sampleSmearEdgeBundleId.asUUID)))
    }
    if (request.sizeAttributeId.nonEmpty) {
      inputs ++= MetaDataSet(Map('sizeAttr -> metaManager.vertexAttribute(request.sizeAttributeId.asUUID)))
    }
    if (request.labelAttributeId.nonEmpty) {
      inputs ++= MetaDataSet(Map('labelAttr -> metaManager.vertexAttribute(request.labelAttributeId.asUUID)))
    }
    val diagramMeta = metaManager.apply(op, inputs)
      .outputs.scalars('svVertices).runtimeSafeCast[Seq[graph_operations.SampledViewVertex]]
    val vertices = dataManager.get(diagramMeta).value

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices.map(v => FEVertex(id = v.id, size = v.size, label = v.label)),
      mode = "sampled")
  }*/

  def getSampledVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    val smearBundle = metaManager.edgeBundle(request.sampleSmearEdgeBundleId.asUUID)
    val center =
      if (request.centralVertexId.isEmpty) None
      else Some(request.centralVertexId.toLong)

    val nop = graph_operations.ComputeVertexNeighborhood(center, request.radius)
    val idToIdx =
      nop(nop.vertices, vertexSet)(nop.edges, smearBundle).result.neighborsIdToIndex.value

    val iaaop = graph_operations.IdAsAttribute()
    val idAttr = iaaop(iaaop.vertices, vertexSet).result.vertexIds
    val fop = graph_operations.VertexAttributeFilter(graph_operations.OneOf(idToIdx.keySet))
    val sample = fop(fop.attr, idAttr).result.fvs
    val filtered = filterMore(sample, request.filters)

    val op = graph_operations.SampledView(
      idToIdx,
      request.sizeAttributeId.nonEmpty,
      request.labelAttributeId.nonEmpty)
    var builder = op(op.vertices, vertexSet)(op.ids, idAttr)(op.filtered, filtered)
    if (request.sizeAttributeId.nonEmpty) {
      val attr = metaManager.vertexAttributeOf[Double](request.sizeAttributeId.asUUID)
      attr.rdd.cache
      builder = builder(op.sizeAttr, attr)
    }
    if (request.labelAttributeId.nonEmpty) {
      val attr = metaManager.vertexAttributeOf[String](request.labelAttributeId.asUUID)
      attr.rdd.cache
      builder = builder(op.labelAttr, attr)
    }
    val diagramMeta = builder.result.svVertices
    val vertices = diagramMeta.value

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices.map(v => FEVertex(id = v.id, size = v.size, label = v.label)),
      mode = "sampled")
  }

  def getDiagramFromBucketedAttributes[S, T](
    original: VertexSet,
    filtered: VertexSet,
    xBucketedAttr: graph_operations.BucketedAttribute[S],
    yBucketedAttr: graph_operations.BucketedAttribute[T]): Scalar[Map[(Int, Int), Int]] = {

    val op = graph_operations.VertexBucketGrid(xBucketedAttr.bucketer, yBucketedAttr.bucketer)
    var builder = op(op.filtered, filtered)(op.vertices, original)
    if (xBucketedAttr.bucketer.numBuckets > 1) {
      builder = builder(op.xAttribute, xBucketedAttr.attribute)
    }
    if (yBucketedAttr.bucketer.numBuckets > 1) {
      builder = builder(op.yAttribute, yBucketedAttr.attribute)
    }
    builder.result.bucketSizes
  }

  def getBucketedVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    val filtered = filter(vertexSet, request.filters)

    val xBucketedAttr = if (request.xNumBuckets > 1 && request.xBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.xBucketingAttributeId.asUUID)
      attribute.rdd.cache
      FEBucketers.bucketedAttribute(metaManager, dataManager, attribute, request.xNumBuckets)
    } else {
      graph_operations.BucketedAttribute[Nothing](
        null, graph_util.EmptyBucketer())
    }
    val yBucketedAttr = if (request.yNumBuckets > 1 && request.yBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.yBucketingAttributeId.asUUID)
      attribute.rdd.cache
      FEBucketers.bucketedAttribute(metaManager, dataManager, attribute, request.yNumBuckets)
    } else {
      graph_operations.BucketedAttribute[Nothing](
        null, graph_util.EmptyBucketer())
    }

    val diagramMeta = getDiagramFromBucketedAttributes(
      vertexSet, filtered, xBucketedAttr, yBucketedAttr)
    val diagram = dataManager.get(diagramMeta).value

    val xBucketer = xBucketedAttr.bucketer
    val yBucketer = yBucketedAttr.bucketer
    val vertices = for (x <- (0 until xBucketer.numBuckets); y <- (0 until yBucketer.numBuckets))
      yield FEVertex(x = x, y = y, size = (diagram.getOrElse((x, y), 0) * 1.0).toInt)

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices,
      mode = "bucketed",
      xLabelType = xBucketer.labelType,
      yLabelType = yBucketer.labelType,
      xLabels = xBucketer.bucketLabels,
      yLabels = yBucketer.bucketLabels)
  }

  private def getCompositeBundle(steps: Seq[BundleSequenceStep]): EdgeAttribute[Double] = {
    val chain = steps.map { step =>
      val bundle = metaManager.edgeBundle(step.bundle.asUUID)
      val directed = if (step.reversed) {
        metaManager.apply(graph_operations.ReverseEdges(), 'esAB -> bundle)
          .outputs.edgeBundles('esBA)
      } else bundle
      metaManager
        .apply(graph_operations.AddConstantDoubleEdgeAttribute(1),
          'edges -> directed)
        .outputs
        .edgeAttributes('attr).runtimeSafeCast[Double]
    }
    return new graph_util.BundleChain(chain).getCompositeEdgeBundle(metaManager)
  }

  private def directVsFromOp(inst: MetaGraphOperationInstance): VertexSet = {
    if (inst.operation.isInstanceOf[graph_operations.SampledView]) {
      inst.outputs.vertexSets('sample)
    } else {
      inst.inputs.vertexSets('vertices)
    }
  }

  private def vsFromOp(inst: MetaGraphOperationInstance): VertexSet = {
    val gridInputVertices = directVsFromOp(inst)
    if (gridInputVertices.source.operation.isInstanceOf[graph_operations.VertexSetIntersection]) {
      val firstIntersected = gridInputVertices.source.inputs.vertexSets('vs0)
      assert(firstIntersected.source.operation
        .isInstanceOf[graph_operations.VertexAttributeFilter[_]])
      firstIntersected.source.inputs.vertexSets('vs)
    } else {
      gridInputVertices
    }
  }

  private def inducedBundle(eb: EdgeBundle,
                            src: VertexSet,
                            dst: VertexSet): EdgeBundle =
    metaManager.apply(
      new graph_operations.InducedEdgeBundle(),
      'edges -> eb,
      'srcSubset -> src,
      'dstSubset -> dst).outputs.edgeBundles('induced)

  private def tripletMapping(eb: EdgeBundle): (VertexAttribute[Array[ID]], VertexAttribute[Array[ID]]) = {
    val metaOut = metaManager.apply(
      graph_operations.TripletMapping(),
      'edges -> eb).outputs
    return (
      metaOut.vertexAttributes('srcEdges).runtimeSafeCast[Array[ID]],
      metaOut.vertexAttributes('dstEdges).runtimeSafeCast[Array[ID]])
  }

  private def idxsFromInst(inst: MetaGraphOperationInstance): VertexAttribute[Int] =
    inst.outputs.vertexAttributes('feIdxs).runtimeSafeCast[Int]

  private def numYBuckets(inst: MetaGraphOperationInstance): Int = {
    inst.operation.asInstanceOf[graph_operations.VertexBucketGrid[_, _]].yBucketer.numBuckets
  }

  private def mappedAttribute[T](mapping: VertexAttribute[Array[ID]],
                                 attr: VertexAttribute[T],
                                 target: EdgeBundle): EdgeAttribute[T] = {
    val op = new graph_operations.VertexToEdgeAttribute[T]()
    val res = op(op.mapping, mapping)(op.original, attr)(op.target, target).result.mappedAttribute
    res.rdd.cache
    res
  }

  def filteredEdgesByAttribute[T](
    eb: EdgeBundle,
    tripletMapping: VertexAttribute[Array[ID]],
    fa: graph_operations.FilteredAttribute[T]): EdgeBundle = {

    val mattr = mappedAttribute(tripletMapping, fa.attribute, eb)
    val fop = graph_operations.EdgeAttributeFilter[T](fa.filter)
    fop(fop.attr, mattr).result.feb
  }

  def indexFromBucketedAttribute[T](
    original: EdgeBundle,
    base: EdgeAttribute[Int],
    tripletMapping: VertexAttribute[Array[ID]],
    ba: graph_operations.BucketedAttribute[T]): EdgeAttribute[Int] = {

    val mattr = mappedAttribute(tripletMapping, ba.attribute, original)

    val iop = graph_operations.EdgeIndexer(ba.bucketer)
    iop(iop.baseIndices, base)(iop.bucketAttribute, mattr).result.indices
  }

  def indexFromIndexingSeq(
    original: EdgeBundle,
    filtered: EdgeBundle,
    tripletMapping: VertexAttribute[Array[ID]],
    seq: Seq[graph_operations.BucketedAttribute[_]]): EdgeAttribute[Int] = {

    val cop = graph_operations.AddConstantIntEdgeAttribute(0)
    val startingBase: EdgeAttribute[Int] = cop(cop.edges, filtered).result.attr
    seq.foldLeft(startingBase) {
      case (b, ba) => indexFromBucketedAttribute(original, b, tripletMapping, ba)
    }
  }

  def getEdgeDiagram(request: EdgeDiagramSpec): EdgeDiagramResponse = {
    val srcView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.srcDiagramId.asUUID))
    val dstView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.dstDiagramId.asUUID))
    val bundleWeights = getCompositeBundle(request.bundleSequence)
    val edgeBundle = bundleWeights.edgeBundle
    val (srcTripletMapping, dstTripletMapping) = tripletMapping(edgeBundle)

    val srcFilteredEBs = srcView.filters
      .map(filteredAttribute =>
        filteredEdgesByAttribute(edgeBundle, srcTripletMapping, filteredAttribute))
    val dstFilteredEBs = dstView.filters
      .map(filteredAttribute =>
        filteredEdgesByAttribute(edgeBundle, dstTripletMapping, filteredAttribute))
    val allFilteredEBs = srcFilteredEBs ++ dstFilteredEBs
    val filteredEB = if (allFilteredEBs.size > 0) {
      val iop = graph_operations.EdgeBundleIntersection(allFilteredEBs.size)
      val builder = allFilteredEBs.zipWithIndex.foldLeft(iop.builder) {
        case (b, (eb, i)) => b(iop.ebs(i), eb)
      }
      builder.result.intersection.entity
    } else {
      edgeBundle
    }

    val srcIndices = indexFromIndexingSeq(
      edgeBundle, filteredEB, srcTripletMapping, srcView.indexingSeq)
    val dstIndices = indexFromIndexingSeq(
      edgeBundle, filteredEB, dstTripletMapping, dstView.indexingSeq)

    val countOp = graph_operations.EdgeIndexPairCounter()
    val counts =
      countOp(countOp.xIndices, srcIndices)(countOp.yIndices, dstIndices).result.counts.value
    EdgeDiagramResponse(
      request.srcDiagramId,
      request.dstDiagramId,
      request.srcIdx,
      request.dstIdx,
      counts.map { case ((s, d), c) => FEEdge(s, d, c) }.toSeq)
  }

  def getComplexView(request: FEGraphRequest): FEGraphRespone = {
    val vertexDiagrams = request.vertexSets.map(getVertexDiagram(_))
    val idxPattern = "idx\\[(\\d+)\\]".r
    def resolveDiagramId(reference: String): String = {
      reference match {
        case idxPattern(idx) => vertexDiagrams(idx.toInt).diagramId
        case id: String => id
      }
    }
    val modifiedEdgeSpecs = request.edgeBundles
      .map(eb => eb.copy(
        srcDiagramId = resolveDiagramId(eb.srcDiagramId),
        dstDiagramId = resolveDiagramId(eb.dstDiagramId)))
    val edgeDiagrams = modifiedEdgeSpecs.map(getEdgeDiagram(_))
    return FEGraphRespone(vertexDiagrams, edgeDiagrams)
  }
}
