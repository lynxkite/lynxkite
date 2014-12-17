package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving.{ FlyingResult, User }
import com.lynxanalytics.biggraph.spark_util

import scala.collection.mutable

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
  id: String = "",
  attrs: Map[String, DynamicValue] = Map())

case class VertexDiagramResponse(
  val diagramId: String,
  val vertices: Seq[FEVertex],
  val mode: String, // as specified in the request

  // ** Only set for bucketed view **
  val xLabelType: String = "",
  val yLabelType: String = "",
  val xLabels: Seq[String] = Seq(),
  val yLabels: Seq[String] = Seq(),
  val xFilters: Seq[String] = Seq(),
  val yFilters: Seq[String] = Seq())

case class EdgeDiagramSpec(
  // In the context of an FEGraphRequest "idx[4]" means the diagram requested by vertexSets(4).
  // Otherwise a UUID obtained by a previous vertex diagram request.
  val srcDiagramId: String,
  val dstDiagramId: String,
  // These are copied verbatim to the response, used by the FE to identify EdgeDiagrams.
  val srcIdx: Int,
  val dstIdx: Int,
  val edgeBundleId: String,
  val filters: Seq[FEVertexAttributeFilter],
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
  edgeBundleId: String = "",
  edgeFilters: Seq[FEVertexAttributeFilter] = Seq())

case class HistogramResponse(
    labelType: String,
    labels: Seq[String],
    sizes: Seq[Long]) {
  val validLabelTypes = Seq("between", "bucket")
  assert(validLabelTypes.contains(labelType),
    s"$labelType is not a valid label type. They are: $validLabelTypes")
}

case class ScalarValueRequest(
  val scalarId: String,
  val calculate: Boolean)

case class CenterRequest(
  vertexSetId: String,
  count: Int,
  filters: Seq[FEVertexAttributeFilter])

case class CenterResponse(
  val centers: Seq[String])

class GraphDrawingController(env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager

  def getVertexDiagram(user: User, request: VertexDiagramSpec): VertexDiagramResponse = {
    request.mode match {
      case "bucketed" => getBucketedVertexDiagram(request)
      case "sampled" => getSampledVertexDiagram(request)
    }
  }

  def getSampledVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    dataManager.cache(vertexSet)
    val centers = request.centralVertexIds.map(_.toLong)

    val idSet = if (request.radius > 0) {
      val smearBundle = metaManager.edgeBundle(request.sampleSmearEdgeBundleId.asUUID)
      dataManager.cache(smearBundle)
      val triplets = tripletMapping(smearBundle, sampled = false)
      val nop = graph_operations.ComputeVertexNeighborhoodFromTriplets(centers, request.radius)
      val nopres = nop(
        nop.vertices, vertexSet)(
          nop.edges, smearBundle)(
            nop.srcTripletMapping, triplets.srcEdges)(
              nop.dstTripletMapping, triplets.dstEdges).result
      nopres.neighborhood.value
    } else {
      centers.toSet
    }

    val iaaop = graph_operations.IdAsAttribute()
    val idAttr = iaaop(iaaop.vertices, vertexSet).result.vertexIds

    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filter(vertexSet, request.filters)

    loadGUIDsToMemory(request.attrs)

    val diagramMeta = {
      val op = graph_operations.SampledView(idSet)
      op(op.vertices, vertexSet)(op.ids, idAttr)(op.filtered, filtered).result.svVertices
    }
    val vertices = diagramMeta.value

    val attrs = request.attrs.map { attrId =>
      attrId -> {
        val attr = metaManager.vertexAttribute(attrId.asUUID)
        val dyn = graph_operations.VertexAttributeToDynamicValue.run(attr)
        val op = graph_operations.CollectAttribute[DynamicValue](idSet)
        op(op.attr, dyn).result.idToAttr.value
      }
    }.toMap

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices.map(v =>
        FEVertex(
          id = v.toString,
          attrs = attrs.mapValues(_.getOrElse(v, DynamicValue(defined = false))))),
      mode = "sampled")
  }

  def getDiagramFromBucketedAttributes[S, T](
    original: VertexSet,
    filtered: VertexSet,
    xBucketedAttr: graph_operations.BucketedAttribute[S],
    yBucketedAttr: graph_operations.BucketedAttribute[T]): Scalar[spark_util.IDBuckets[(Int, Int)]] = {

    val cop = graph_operations.CountVertices()
    val originalCount = cop(cop.vertices, original).result.count
    val op = graph_operations.VertexBucketGrid(xBucketedAttr.bucketer, yBucketedAttr.bucketer)
    var builder = op(op.filtered, filtered)(op.vertices, original)(op.originalCount, originalCount)
    if (xBucketedAttr.nonEmpty) {
      builder = builder(op.xAttribute, xBucketedAttr.attribute)
    }
    if (yBucketedAttr.nonEmpty) {
      builder = builder(op.yAttribute, yBucketedAttr.attribute)
    }
    builder.result.buckets
  }

  def getBucketedVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    dataManager.cache(vertexSet)
    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filter(vertexSet, request.filters)

    val xBucketedAttr = if (request.xBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.xBucketingAttributeId.asUUID)
      dataManager.cache(attribute)
      FEBucketers.bucketedAttribute(
        metaManager, dataManager, attribute, request.xNumBuckets, request.xAxisOptions)
    } else {
      graph_operations.BucketedAttribute.emptyBucketedAttribute
    }
    val yBucketedAttr = if (request.yBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.yBucketingAttributeId.asUUID)
      dataManager.cache(attribute)
      FEBucketers.bucketedAttribute(
        metaManager, dataManager, attribute, request.yNumBuckets, request.yAxisOptions)
    } else {
      graph_operations.BucketedAttribute.emptyBucketedAttribute
    }

    val diagramMeta = getDiagramFromBucketedAttributes(
      vertexSet, filtered, xBucketedAttr, yBucketedAttr)
    val diagram = dataManager.get(diagramMeta).value

    val xBucketer = xBucketedAttr.bucketer
    val yBucketer = yBucketedAttr.bucketer
    val vertices = for (x <- (0 until xBucketer.numBuckets); y <- (0 until yBucketer.numBuckets))
      yield FEVertex(x = x, y = y, size = diagram.counts((x, y)))

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices,
      mode = "bucketed",
      xLabelType = xBucketer.labelType,
      yLabelType = yBucketer.labelType,
      xLabels = xBucketer.bucketLabels,
      yLabels = yBucketer.bucketLabels,
      xFilters = xBucketer.bucketFilters,
      yFilters = yBucketer.bucketFilters)
  }

  private def loadGUIDsToMemory(gUIDs: Seq[String]): Unit = {
    gUIDs.foreach(id => dataManager.cache(metaManager.entity(id.asUUID)))
  }

  private def tripletMapping(
    eb: EdgeBundle, sampled: Boolean): graph_operations.TripletMapping.Output = {
    val op =
      if (sampled) graph_operations.TripletMapping(sampleSize = 500000)
      else graph_operations.TripletMapping()
    val res = op(op.edges, eb).result
    dataManager.cache(res.srcEdges)
    dataManager.cache(res.dstEdges)
    return res
  }

  private def mappedAttribute[T](mapping: Attribute[Array[ID]],
                                 attr: Attribute[T],
                                 target: EdgeBundle): Attribute[T] = {
    val op = new graph_operations.VertexToEdgeAttribute[T]()
    val res = op(op.mapping, mapping)(op.original, attr)(op.target, target).result.mappedAttribute
    dataManager.cache(res)
    res
  }

  def filteredEdgeIdsByAttribute[T](
    eb: EdgeBundle,
    tripletMapping: Attribute[Array[ID]],
    fa: graph_operations.FilteredAttribute[T]): VertexSet = {

    val mattr = mappedAttribute(tripletMapping, fa.attribute, eb)
    val fop = graph_operations.VertexAttributeFilter[T](fa.filter)
    fop(fop.attr, mattr).result.fvs
  }

  def indexFromBucketedAttribute[T](
    base: Attribute[Int],
    ba: graph_operations.BucketedAttribute[T]): Attribute[Int] = {

    val iop = graph_operations.Indexer(ba.bucketer)
    iop(iop.baseIndices, base)(iop.bucketAttribute, ba.attribute).result.indices
  }

  def indexFromIndexingSeq(
    filtered: VertexSet,
    seq: Seq[graph_operations.BucketedAttribute[_]]): Attribute[Int] = {

    val startingBase: Attribute[Int] = graph_operations.AddConstantAttribute.run(filtered, 0)
    seq.foldLeft(startingBase) { case (b, ba) => indexFromBucketedAttribute(b, ba) }
  }

  def edgeIndexFromBucketedAttribute[T](
    original: EdgeBundle,
    base: Attribute[Int],
    tripletMapping: Attribute[Array[ID]],
    ba: graph_operations.BucketedAttribute[T]): Attribute[Int] = {

    val mattr = mappedAttribute(tripletMapping, ba.attribute, original)

    val iop = graph_operations.Indexer(ba.bucketer)
    iop(iop.baseIndices, base)(iop.bucketAttribute, mattr).result.indices
  }

  def edgeIndexFromIndexingSeq(
    original: EdgeBundle,
    filteredIds: VertexSet,
    tripletMapping: Attribute[Array[ID]],
    seq: Seq[graph_operations.BucketedAttribute[_]]): Attribute[Int] = {

    val startingBase: Attribute[Int] =
      graph_operations.AddConstantAttribute.run(filteredIds, 0)
    seq.foldLeft(startingBase) {
      case (b, ba) => edgeIndexFromBucketedAttribute(original, b, tripletMapping, ba)
    }
  }

  def getSmallEdgeSet(
    eb: EdgeBundle,
    srcView: graph_operations.VertexView,
    dstView: graph_operations.VertexView): Option[Seq[(ID, Edge)]] = {

    val tm = tripletMapping(eb, sampled = false)
    if (srcView.vertexIndices.isDefined) {
      val vertexIds = srcView.vertexIndices.get.keySet
      val op = graph_operations.EdgesForVertices(vertexIds, 50000, bySource = true)
      val edges =
        op(op.edges, eb)(op.tripletMapping, tm.srcEdges).result.edges.value
      if (edges.isDefined) return edges
    }
    if (dstView.vertexIndices.isDefined) {
      val vertexIds = dstView.vertexIndices.get.keySet
      val op = graph_operations.EdgesForVertices(vertexIds, 50000, bySource = false)
      val edges =
        op(op.edges, eb)(op.tripletMapping, tm.dstEdges).result.edges.value
      if (edges.isDefined) return edges
    }
    return None
  }

  def getEdgeDiagram(user: User, request: EdgeDiagramSpec): EdgeDiagramResponse = {
    val srcView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.srcDiagramId.asUUID))
    val dstView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.dstDiagramId.asUUID))
    val edgeBundle = metaManager.edgeBundle(request.edgeBundleId.asUUID)
    dataManager.cache(edgeBundle)
    val weights = if (request.edgeWeightId.isEmpty) {
      graph_operations.AddConstantAttribute.run(edgeBundle.asVertexSet, 1.0)
    } else {
      val w = metaManager.vertexAttributeOf[Double](request.edgeWeightId.asUUID)
      dataManager.cache(w)
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

    val smallEdgeSetOption = getSmallEdgeSet(edgeBundle, srcView, dstView)
    val counts = smallEdgeSetOption match {
      case Some(smallEdgeSet) => {
        log.info("PERF Small edge set mode for request: " + request)
        val smallEdgeSetMap = smallEdgeSet.toMap
        val filteredEdgeSetIDs = FEFilters.localFilter(smallEdgeSetMap.keySet, request.filters)
        val filteredEdgeSet = filteredEdgeSetIDs.map(id => id -> smallEdgeSetMap(id))
        val srcIdxMapping = srcView.vertexIndices.getOrElse {
          val indexAttr = indexFromIndexingSeq(srcView.vertexSet, srcView.indexingSeq)
          val srcVertexIds = filteredEdgeSet.map { case (id, edge) => edge.src }.toSet
          graph_operations.RestrictAttributeToIds.run(indexAttr, srcVertexIds).value.toMap
        }
        val dstIdxMapping = dstView.vertexIndices.getOrElse {
          val indexAttr = indexFromIndexingSeq(dstView.vertexSet, dstView.indexingSeq)
          val dstVertexIds = filteredEdgeSet.map { case (id, edge) => edge.dst }.toSet
          graph_operations.RestrictAttributeToIds.run(indexAttr, dstVertexIds).value.toMap
        }
        val weightMap = graph_operations.RestrictAttributeToIds.run(
          weights, filteredEdgeSet.map(_._1).toSet).value.toMap
        val counts = mutable.Map[(Int, Int), Double]().withDefaultValue(0.0)
        filteredEdgeSet.foreach {
          case (id, edge) =>
            val src = edge.src
            val dst = edge.dst
            if (srcIdxMapping.contains(src) && dstIdxMapping.contains(dst)) {
              counts((srcIdxMapping(src), dstIdxMapping(dst))) += weightMap(id)
            }
        }
        counts.toMap
      }
      case None => {
        log.info("PERF Huge edge set mode for request: " + request)
        val vertexFiltered = getFilteredEdgeIds(edgeBundle, srcView.filters, dstView.filters)
        val edgeFiltered = getFilteredVS(vertexFiltered.ids, request.filters)

        val srcIndices = edgeIndexFromIndexingSeq(
          edgeBundle, edgeFiltered, vertexFiltered.srcTripletMapping, srcView.indexingSeq)
        val dstIndices = edgeIndexFromIndexingSeq(
          edgeBundle, edgeFiltered, vertexFiltered.dstTripletMapping, dstView.indexingSeq)

        val cop = graph_operations.CountEdges()
        val originalEdgeCount = cop(cop.edges, edgeBundle).result.count
        val countOp = graph_operations.IndexPairCounter()
        countOp(
          countOp.xIndices, srcIndices)(
            countOp.yIndices, dstIndices)(
              countOp.original, edgeBundle.asVertexSet)(
                countOp.weights, weights)(
                  countOp.originalCount, originalEdgeCount).result.counts.value
      }
    }
    log.info("PERF edge counts computed")
    EdgeDiagramResponse(
      request.srcDiagramId,
      request.dstDiagramId,
      request.srcIdx,
      request.dstIdx,
      counts.map { case ((s, d), c) => FEEdge(s, d, c) }.toSeq)
  }

  def getComplexView(user: User, request: FEGraphRequest): FEGraphResponse = {
    val vertexDiagrams = request.vertexSets.map(getVertexDiagram(user, _))
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
    val edgeDiagrams = modifiedEdgeSpecs.map(getEdgeDiagram(user, _))
    spark_util.Counters.printAll
    return FEGraphResponse(vertexDiagrams, edgeDiagrams)
  }

  private def getFilteredVS(
    vertexSet: VertexSet,
    vertexFilters: Seq[FEVertexAttributeFilter]): VertexSet = {

    loadGUIDsToMemory(vertexFilters.map(_.attributeId))
    FEFilters.filter(vertexSet, vertexFilters)
  }

  private case class FilteredEdges(
    ids: VertexSet,
    srcTripletMapping: Attribute[Array[ID]],
    dstTripletMapping: Attribute[Array[ID]])

  private def getFilteredEdgeIds(
    edgeBundle: EdgeBundle,
    srcFilters: Seq[graph_operations.FilteredAttribute[_]],
    dstFilters: Seq[graph_operations.FilteredAttribute[_]]): FilteredEdges = {
    val sampledTrips = tripletMapping(edgeBundle, sampled = true)
    val sampledEdges = getFilteredEdgeIds(sampledTrips, edgeBundle, srcFilters, dstFilters)
    // TODO: See if we can eliminate the extra stage from this "count".
    val count = {
      val op = graph_operations.CountVertices()
      op(op.vertices, sampledEdges.ids).result.count.value
    }
    if (count >= 50000) {
      sampledEdges
    } else {
      val fullTrips = tripletMapping(edgeBundle, sampled = false)
      getFilteredEdgeIds(fullTrips, edgeBundle, srcFilters, dstFilters)
    }
  }

  private def getFilteredEdgeIds(
    trips: graph_operations.TripletMapping.Output,
    edgeBundle: EdgeBundle,
    srcFilters: Seq[graph_operations.FilteredAttribute[_]],
    dstFilters: Seq[graph_operations.FilteredAttribute[_]]): FilteredEdges = {
    val srcFilteredIds = srcFilters
      .map(filteredAttribute =>
        filteredEdgeIdsByAttribute(edgeBundle, trips.srcEdges, filteredAttribute))
    val dstFilteredIds = dstFilters
      .map(filteredAttribute =>
        filteredEdgeIdsByAttribute(edgeBundle, trips.dstEdges, filteredAttribute))
    val allFilteredIds = srcFilteredIds ++ dstFilteredIds
    val ids = if (allFilteredIds.size > 0) {
      val iop = graph_operations.VertexSetIntersection(allFilteredIds.size)
      iop(iop.vss, allFilteredIds).result.intersection.entity
    } else {
      edgeBundle.asVertexSet
    }
    FilteredEdges(ids, trips.srcEdges, trips.dstEdges)
  }

  def getCenter(user: User, request: CenterRequest): CenterResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    dataManager.cache(vertexSet)
    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filter(vertexSet, request.filters)
    val sampled = {
      val op = graph_operations.SampleVertices(request.count)
      op(op.vs, filtered).result.sample.value
    }
    CenterResponse(sampled.map(_.toString))
  }

  def getHistogram(user: User, request: HistogramSpec): HistogramResponse = {
    val vertexAttribute = metaManager.vertexAttribute(request.attributeId.asUUID)
    dataManager.cache(vertexAttribute.vertexSet)
    dataManager.cache(vertexAttribute)
    loadGUIDsToMemory(request.vertexFilters.map(_.attributeId))
    val bucketedAttr = FEBucketers.bucketedAttribute(
      metaManager, dataManager, vertexAttribute, request.numBuckets, request.axisOptions)
    val filteredVS = if (request.edgeBundleId.isEmpty) {
      getFilteredVS(vertexAttribute.vertexSet, request.vertexFilters)
    } else {
      val edgeBundle = metaManager.edgeBundle(request.edgeBundleId.asUUID)
      val filters = request.vertexFilters.map(_.toFilteredAttribute)
      val vertexFiltered = getFilteredEdgeIds(edgeBundle, filters, filters).ids
      getFilteredVS(vertexFiltered, request.edgeFilters)
    }
    val histogram = bucketedAttr.toHistogram(filteredVS)
    val counts = histogram.counts.value
    spark_util.Counters.printAll
    HistogramResponse(
      bucketedAttr.bucketer.labelType,
      bucketedAttr.bucketer.bucketLabels,
      (0 until bucketedAttr.bucketer.numBuckets).map(counts.getOrElse(_, 0L)))
  }

  def getScalarValue(user: User, request: ScalarValueRequest): DynamicValue = {
    val scalar = metaManager.scalar(request.scalarId.asUUID)
    if (!request.calculate && !dataManager.isCalculated(scalar)) {
      throw new FlyingResult(play.api.mvc.Results.NotFound("Value is not calculated yet"))
    }
    dynamicValue(scalar)
  }

  private def dynamicValue[T](scalar: Scalar[T]) = {
    implicit val tt = scalar.typeTag
    graph_operations.DynamicValue.convert(scalar.value)
  }
}
