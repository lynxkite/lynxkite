package com.lynxanalytics.biggraph.controllers

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util

case class VertexDiagramSpec(
  val vertexSetId: String,
  val filters: Seq[FEVertexAttributeFilter],
  val mode: String, // For now, one of "bucketed", "sampled".

  // ** Parameters for bucketed view **
  // Empty string means no bucketing on that axis.
  val xBucketingAttributeId: String = "",
  val xNumBuckets: Int = 1,
  val xAxisOptions: AxisOptions = AxisOptions(),
  val yBucketingAttributeId: String = "",
  val yNumBuckets: Int = 1,
  val yAxisOptions: AxisOptions = AxisOptions(),

  // ** Parameters for sampled view **
  val centralVertexIds: Seq[String] = Seq(),
  // Edge bundle used to find neighborhood of the central vertex.
  val sampleSmearEdgeBundleId: String = "",
  val attrs: Seq[String] = Seq(),
  val radius: Int = 1)

case class FEVertex(
  // For bucketed view:
  size: Double = 0.0,
  x: Int = 0,
  y: Int = 0,

  // For sampled view:
  id: Long = 0,
  attrs: Map[String, DynamicValue] = Map())

case class VertexDiagramResponse(
  val diagramId: String,
  val vertices: Seq[FEVertex],
  val mode: String, // as specified in the request

  // ** Only set for bucketed view **
  val xLabelType: String = "",
  val yLabelType: String = "",
  val xLabels: Seq[String] = Seq(),
  val yLabels: Seq[String] = Seq())

case class EdgeDiagramSpec(
  // In the context of an FEGraphRequest "idx[4]" means the diagram requested by vertexSets(4).
  // Otherwise a UUID obtained by a previous vertex diagram request.
  val srcDiagramId: String,
  val dstDiagramId: String,
  // These are copied verbatim to the response, used by the FE to identify EdgeDiagrams.
  val srcIdx: Int,
  val dstIdx: Int,
  val edgeBundleId: String,
  // If not set, we use constant 1 as weight.
  val edgeWeightId: String = "")

case class BundleSequenceStep(bundle: String, reversed: Boolean)

case class FEEdge(
  // idx of source vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  a: Int,
  // idx of destination vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  b: Int,
  size: Double)

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

case class FEGraphResponse(
  vertexSets: Seq[VertexDiagramResponse],
  edgeBundles: Seq[EdgeDiagramResponse])

case class AxisOptions(
  logarithmic: Boolean = false)

case class HistogramSpec(
  attributeId: String,
  vertexFilters: Seq[FEVertexAttributeFilter],
  numBuckets: Int,
  axisOptions: AxisOptions,
  // Set only if we ask for an edge attribute histogram and provided vertexFilters should be
  // applied on the end-vertices of edges.
  edgeBundleId: String = "")

case class HistogramResponse(
    labelType: String,
    labels: Seq[String],
    sizes: Seq[Int]) {
  val validLabelTypes = Seq("between", "bucket")
  assert(validLabelTypes.contains(labelType),
    s"$labelType is not a valid label type. They are: $validLabelTypes")
}

case class ScalarValueRequest(
  val scalarId: String,
  val calculate: Boolean)

case class ScalarValueResponse(
  val value: String)

case class CenterRequest(
  vertexSetId: String,
  count: Int,
  filters: Seq[FEVertexAttributeFilter])

case class CenterResponse(
  val centers: Seq[String])

class GraphDrawingController(env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager

  def getVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    request.mode match {
      case "bucketed" => getBucketedVertexDiagram(request)
      case "sampled" => getSampledVertexDiagram(request)
    }
  }

  def getSampledVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    dataManager.loadToMemory(vertexSet)
    val centers = request.centralVertexIds.map(_.toLong)

    val ids = if (request.radius > 0) {
      val smearBundle = metaManager.edgeBundle(request.sampleSmearEdgeBundleId.asUUID)
      dataManager.loadToMemory(smearBundle)
      val nop = graph_operations.ComputeVertexNeighborhood(centers, request.radius)
      val nopres = nop(nop.vertices, vertexSet)(nop.edges, smearBundle).result
      nopres.neighborhood.value
    } else {
      centers.toSet
    }

    val iaaop = graph_operations.IdAsAttribute()
    val idAttr = iaaop(iaaop.vertices, vertexSet).result.vertexIds

    val fop = graph_operations.VertexAttributeFilter(graph_operations.OneOf(ids))
    val sample = fop(fop.attr, idAttr).result.fvs

    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filterMore(sample, request.filters)

    loadGUIDsToMemory(request.attrs)
    val attrs = request.attrs.map(x => metaManager.vertexAttribute(x.asUUID))
    val dynAttrs = attrs.map(graph_operations.VertexAttributeToDynamicValue.run(_))

    val op = graph_operations.SampledView(dynAttrs.size > 0)
    var builder = op(op.vertices, vertexSet)(op.ids, idAttr)(op.filtered, filtered)
    if (dynAttrs.size > 0) {
      val joined = {
        val op = graph_operations.JoinMoreAttributes(dynAttrs.size, DynamicValue())
        op(op.vs, vertexSet)(op.attrs, dynAttrs).result.attr.entity
      }
      builder = builder(op.attr, joined)
    }
    val diagramMeta = builder.result.svVertices

    val vertices = diagramMeta.value

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices.map(v =>
        FEVertex(
          id = v.id,
          attrs = request.attrs.zip(v.attrs).toMap)),
      mode = "sampled")
  }

  def getDiagramFromBucketedAttributes[S, T](
    original: VertexSet,
    filtered: VertexSet,
    xBucketedAttr: graph_operations.BucketedAttribute[S],
    yBucketedAttr: graph_operations.BucketedAttribute[T]): Scalar[Map[(Int, Int), Int]] = {

    val cop = graph_operations.CountVertices()
    val originalCount = cop(cop.vertices, original).result.count
    val op = graph_operations.VertexBucketGrid(xBucketedAttr.bucketer, yBucketedAttr.bucketer)
    var builder = op(op.filtered, filtered)(op.vertices, original)(op.originalCount, originalCount)
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
    dataManager.loadToMemory(vertexSet)
    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filter(vertexSet, request.filters)

    val xBucketedAttr = if (request.xNumBuckets > 1 && request.xBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.xBucketingAttributeId.asUUID)
      dataManager.loadToMemory(attribute)
      FEBucketers.bucketedAttribute(
        metaManager, dataManager, attribute, request.xNumBuckets, request.xAxisOptions)
    } else {
      graph_operations.BucketedAttribute[Nothing](
        null, graph_util.EmptyBucketer())
    }
    val yBucketedAttr = if (request.yNumBuckets > 1 && request.yBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.yBucketingAttributeId.asUUID)
      dataManager.loadToMemory(attribute)
      FEBucketers.bucketedAttribute(
        metaManager, dataManager, attribute, request.yNumBuckets, request.yAxisOptions)
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

  private def loadGUIDsToMemory(gUIDs: Seq[String]): Unit = {
    gUIDs.foreach(id => dataManager.loadToMemory(metaManager.entity(id.asUUID)))
  }

  private def tripletMapping(eb: EdgeBundle): (VertexAttribute[Array[ID]], VertexAttribute[Array[ID]]) = {
    val op = graph_operations.TripletMapping()
    val res = op(op.edges, eb).result
    val srcMapping = res.srcEdges
    val dstMapping = res.dstEdges
    dataManager.loadToMemory(srcMapping)
    dataManager.loadToMemory(dstMapping)
    return (srcMapping, dstMapping)
  }

  private def mappedAttribute[T](mapping: VertexAttribute[Array[ID]],
                                 attr: VertexAttribute[T],
                                 target: EdgeBundle): VertexAttribute[T] = {
    val op = new graph_operations.VertexToEdgeAttribute[T]()
    val res = op(op.mapping, mapping)(op.original, attr)(op.target, target).result.mappedAttribute
    dataManager.loadToMemory(res)
    res
  }

  def filteredEdgeIdsByAttribute[T](
    eb: EdgeBundle,
    tripletMapping: VertexAttribute[Array[ID]],
    fa: graph_operations.FilteredAttribute[T]): VertexSet = {

    val mattr = mappedAttribute(tripletMapping, fa.attribute, eb)
    val fop = graph_operations.VertexAttributeFilter[T](fa.filter)
    fop(fop.attr, mattr).result.fvs
  }

  def indexFromBucketedAttribute[T](
    original: EdgeBundle,
    base: VertexAttribute[Int],
    tripletMapping: VertexAttribute[Array[ID]],
    ba: graph_operations.BucketedAttribute[T]): VertexAttribute[Int] = {

    val mattr = mappedAttribute(tripletMapping, ba.attribute, original)

    val iop = graph_operations.Indexer(ba.bucketer)
    iop(iop.baseIndices, base)(iop.bucketAttribute, mattr).result.indices
  }

  def indexFromIndexingSeq(
    original: EdgeBundle,
    filteredIds: VertexSet,
    tripletMapping: VertexAttribute[Array[ID]],
    seq: Seq[graph_operations.BucketedAttribute[_]]): VertexAttribute[Int] = {

    val startingBase: VertexAttribute[Int] =
      graph_operations.AddConstantAttribute.run(filteredIds, 0)
    seq.foldLeft(startingBase) {
      case (b, ba) => indexFromBucketedAttribute(original, b, tripletMapping, ba)
    }
  }

  def getEdgeDiagram(request: EdgeDiagramSpec): EdgeDiagramResponse = {
    val srcView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.srcDiagramId.asUUID))
    val dstView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.dstDiagramId.asUUID))
    val edgeBundle = metaManager.edgeBundle(request.edgeBundleId.asUUID)
    dataManager.loadToMemory(edgeBundle)
    val weights = if (request.edgeWeightId.isEmpty) {
      graph_operations.AddConstantAttribute.run(edgeBundle.asVertexSet, 1.0)
    } else {
      val w = metaManager.vertexAttributeOf[Double](request.edgeWeightId.asUUID)
      dataManager.loadToMemory(w)
      w
    }
    assert(
      weights.vertexSet == edgeBundle.asVertexSet,
      "The requested edge weight attribute does not belong to the requested edge bundle.\n" +
        "Edge bundle: $edgeBundle\nWeight attribute: $weights")
    assert(srcView.vertexSet.gUID == edgeBundle.srcVertexSet.gUID,
      "Source vertex set does not match edge bundle source." +
        s"\nSource: ${srcView.vertexSet}\nEdge bundle source: ${edgeBundle.srcVertexSet}")
    assert(dstView.vertexSet.gUID == edgeBundle.dstVertexSet.gUID,
      "Destination vertex set does not match edge bundle destination." +
        s"\nSource: ${dstView.vertexSet}\nEdge bundle destination: ${edgeBundle.dstVertexSet}")
    val (srcTripletMapping, dstTripletMapping) = tripletMapping(edgeBundle)

    val srcFilteredIds = srcView.filters
      .map(filteredAttribute =>
        filteredEdgeIdsByAttribute(edgeBundle, srcTripletMapping, filteredAttribute))
    val dstFilteredIds = dstView.filters
      .map(filteredAttribute =>
        filteredEdgeIdsByAttribute(edgeBundle, dstTripletMapping, filteredAttribute))
    val allFilteredIds = srcFilteredIds ++ dstFilteredIds
    val filteredIds = if (allFilteredIds.size > 0) {
      val iop = graph_operations.VertexSetIntersection(allFilteredIds.size)
      iop(iop.vss, allFilteredIds).result.intersection.entity
    } else {
      edgeBundle.asVertexSet
    }

    val srcIndices = indexFromIndexingSeq(
      edgeBundle, filteredIds, srcTripletMapping, srcView.indexingSeq)
    val dstIndices = indexFromIndexingSeq(
      edgeBundle, filteredIds, dstTripletMapping, dstView.indexingSeq)

    val cop = graph_operations.CountEdges()
    val originalEdgeCount = cop(cop.edges, edgeBundle).result.count
    val countOp = graph_operations.IndexPairCounter()
    val counts =
      countOp(
        countOp.xIndices, srcIndices)(
          countOp.yIndices, dstIndices)(
            countOp.original, edgeBundle.asVertexSet)(
              countOp.weights, weights)(
                countOp.originalCount, originalEdgeCount).result.counts.value
    EdgeDiagramResponse(
      request.srcDiagramId,
      request.dstDiagramId,
      request.srcIdx,
      request.dstIdx,
      counts.map { case ((s, d), c) => FEEdge(s, d, c) }.toSeq)
  }

  def getComplexView(request: FEGraphRequest): FEGraphResponse = {
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
    spark_util.Counters.printAll
    return FEGraphResponse(vertexDiagrams, edgeDiagrams)
  }

  private def getFilteredVS(
    vertexSet: VertexSet,
    vertexFilters: Seq[FEVertexAttributeFilter]): VertexSet = {

    loadGUIDsToMemory(vertexFilters.map(_.attributeId))
    FEFilters.filter(vertexSet, vertexFilters)
  }

  private def getFilteredEdgeIds(
    edgeBundle: EdgeBundle,
    vertexFilters: Seq[FEVertexAttributeFilter]): VertexSet = {

    val (srcTripletMapping, dstTripletMapping) = tripletMapping(edgeBundle)
    val filteredIdSets = vertexFilters
      .map(_.toFilteredAttribute)
      .flatMap(filteredAttribute =>
        Iterator(
          filteredEdgeIdsByAttribute(edgeBundle, srcTripletMapping, filteredAttribute),
          filteredEdgeIdsByAttribute(edgeBundle, dstTripletMapping, filteredAttribute)))

    if (filteredIdSets.size > 0) {
      val iop = graph_operations.VertexSetIntersection(filteredIdSets.size)
      iop(iop.vss, filteredIdSets).result.intersection.entity
    } else {
      edgeBundle.asVertexSet
    }
  }

  def getCenter(request: CenterRequest): CenterResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    dataManager.loadToMemory(vertexSet)
    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filter(vertexSet, request.filters)
    val sampled = {
      val op = graph_operations.SampleVertices(request.count)
      op(op.vs, filtered).result.sample.value
    }
    CenterResponse(sampled.map(_.toString))
  }

  def getHistogram(request: HistogramSpec): HistogramResponse = {
    val vertexAttribute = metaManager.vertexAttribute(request.attributeId.asUUID)
    dataManager.loadToMemory(vertexAttribute)
    loadGUIDsToMemory(request.vertexFilters.map(_.attributeId))
    val bucketedAttr = FEBucketers.bucketedAttribute(
      metaManager, dataManager, vertexAttribute, request.numBuckets, request.axisOptions)
    val filteredVS = if (request.edgeBundleId.isEmpty) {
      getFilteredVS(vertexAttribute.vertexSet, request.vertexFilters)
    } else {
      getFilteredEdgeIds(
        metaManager.edgeBundle(request.edgeBundleId.asUUID), request.vertexFilters)
    }
    val histogram = bucketedAttr.toHistogram(filteredVS)
    val counts = histogram.counts.value
    HistogramResponse(
      bucketedAttr.bucketer.labelType,
      bucketedAttr.bucketer.bucketLabels,
      (0 until bucketedAttr.bucketer.numBuckets).map(counts.getOrElse(_, 0)))
  }

  def getScalarValue(request: ScalarValueRequest): ScalarValueResponse = {
    val scalar = metaManager.scalar(request.scalarId.asUUID)
    assert(request.calculate || dataManager.isCalculated(scalar), "Value is not calculated yet")
    ScalarValueResponse(scalar.value.toString)
  }
}
