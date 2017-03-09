// Request handlers that need to use the DataManager.
//
// Most notably it includes everything for the bucketed and sampled graphs,
// but it is also used for the histograms and scalars.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.model
import com.lynxanalytics.biggraph.serving.User
import com.lynxanalytics.biggraph.spark_util

import scala.collection.mutable
import scala.concurrent.Future

object DrawingThresholds {
  private def get(suffix: String, default: Int): Int = {
    LoggedEnvironment.envOrNone("KITE_DRAWING_" + suffix).map(_.toInt).getOrElse(default)
  }
  val Overall = LoggedEnvironment.envOrElse("KITE_DRAWING_OVERALL", "10000").toInt
  val BucketSampling = get("BUCKET_SAMPLING", Overall * 5)
  val MaxSampledViewVertices = get("MAX_SAMPLED_VIEW_VERTICES", Overall)
  val MaxSampledViewEdges = get("MAX_SAMPLED_VIEW_EDGES", Overall)
  val TripletSampling = get("TRIPLET_SAMPLING", Overall * 50)
  val SmallEdges = get("SMALL_EDGES", Overall * 5)
  val FilterMinRemaining = get("FILTER_MIN_REMAINING", Overall * 5)
}

case class VertexDiagramSpec(
  vertexSetId: String,
  filters: Seq[FEVertexAttributeFilter],
  mode: String, // For now, one of "bucketed", "sampled".

  // ** Parameters for bucketed view **
  // Empty string means no bucketing on that axis.
  xBucketingAttributeId: String = "",
  xNumBuckets: Int = 1,
  xAxisOptions: AxisOptions = AxisOptions(),
  yBucketingAttributeId: String = "",
  yNumBuckets: Int = 1,
  yAxisOptions: AxisOptions = AxisOptions(),
  sampleSize: Int = DrawingThresholds.BucketSampling,

  // ** Parameters for sampled view **
  centralVertexIds: Seq[String] = Seq(),
  // Edge bundle used to find neighborhood of the central vertex.
  sampleSmearEdgeBundleId: String = "",
  attrs: Seq[String] = Seq(),
  radius: Int = 1,
  maxSize: Int = DrawingThresholds.MaxSampledViewVertices)

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
    val yFilters: Seq[String] = Seq()) {

  def size = vertices.size
}

case class AggregatedAttribute(
  // The GUID of the attribute.
  attributeId: String,
  // The aggregation we want to apply on it.
  aggregator: String)

case class EdgeDiagramSpec(
  // In the context of an FEGraphRequest "idx[4]" means the diagram requested by vertexSets(4).
  // Otherwise a UUID obtained by a previous vertex diagram request.
  srcDiagramId: String,
  dstDiagramId: String,
  // These are copied verbatim to the response, used by the FE to identify EdgeDiagrams.
  srcIdx: Int,
  dstIdx: Int,
  // The GUID of the edge bundle to plot.
  edgeBundleId: String,
  // Specification of filters that should be applied to attributes of the edge bundle.
  filters: Seq[FEVertexAttributeFilter],
  // If not set, we use constant 1 as weight.
  edgeWeightId: String = "",
  // Whether to generate 3D coordinates for the vertices.
  layout3D: Boolean,
  //whether to normalize the thickness of the edges shown on the bucketed graph according to the relative density
  relativeEdgeDensity: Boolean,
  // Attributes to be returned together with the edges. As one visualized edge can correspond to
  // many actual edges, clients always have to specify an aggregator as well. For now, this only
  // works for small edge set visualizations (i.e. sampled mode).
  attrs: Seq[AggregatedAttribute] = Seq(),
  maxSize: Int = DrawingThresholds.MaxSampledViewEdges)

case class BundleSequenceStep(bundle: String, reversed: Boolean)

case class FEEdge(
  // idx of source vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  a: Int,
  // idx of destination vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  b: Int,
  size: Double,
  // Keys are composed as attributeId:aggregator.
  attrs: Map[String, DynamicValue] = Map())

case class FE3DPosition(x: Double, y: Double, z: Double)

case class EdgeDiagramResponse(
    srcDiagramId: String,
    dstDiagramId: String,

    // Copied from the request.
    srcIdx: Int,
    dstIdx: Int,

    edges: Seq[FEEdge],

    // The vertex coordinates, if "layout3D" was true in the request.
    layout3D: Map[String, FE3DPosition]) {

  def size = edges.size
}

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
  edgeFilters: Seq[FEVertexAttributeFilter] = Seq(),
  sampleSize: Int)

case class HistogramResponse(
    labelType: String,
    labels: Seq[String],
    sizes: Seq[Long]) {
  val validLabelTypes = Seq("between", "bucket")
  assert(validLabelTypes.contains(labelType),
    s"$labelType is not a valid label type. They are: $validLabelTypes")
}

case class ScalarValueRequest(
  scalarId: String)

case class CenterRequest(
  vertexSetId: String,
  count: Int,
  filters: Seq[FEVertexAttributeFilter])

case class CenterResponse(
  val centers: Seq[String])

class GraphDrawingController(env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager
  import dataManager.executionContext

  def getVertexDiagram(user: User, request: VertexDiagramSpec): VertexDiagramResponse = {
    request.mode match {
      case "bucketed" => getBucketedVertexDiagram(request)
      case "sampled" => getSampledVertexDiagram(request)
    }
  }

  def getSampledVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    dataManager.cache(vertexSet)

    val iaaop = graph_operations.IdAsAttribute()
    val idAttr = iaaop(iaaop.vertices, vertexSet).result.vertexIds
    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filter(vertexSet, request.filters)
    loadGUIDsToMemory(request.attrs)

    val centers = if (request.centralVertexIds == Seq("*")) {
      // Try to show the whole graph.
      val op = graph_operations.SampleVertices(request.maxSize + 1)
      val sample = op(op.vs, filtered).result.sample.value
      assert(
        sample.size <= request.maxSize,
        s"The full graph is too large to display (larger than ${request.maxSize}).")
      sample
    } else {
      request.centralVertexIds.map(_.toLong)
    }

    val idSet = if (request.radius > 0) {
      val smearBundle = metaManager.edgeBundle(request.sampleSmearEdgeBundleId.asUUID)
      dataManager.cache(smearBundle)
      val edgesAndNeighbors = edgesAndNeighborsMapping(smearBundle, sampled = false)
      val nop = graph_operations.ComputeVertexNeighborhoodFromEdgesAndNeighbors(
        centers, request.radius, request.maxSize)
      val nopres = nop(
        nop.vertices, vertexSet)(
          nop.srcMapping, edgesAndNeighbors.srcEdges)(
            nop.dstMapping, edgesAndNeighbors.dstEdges).result
      val neighborhood = nopres.neighborhood.value
      assert(
        centers.isEmpty || neighborhood.nonEmpty,
        s"Neighborhood is too large to display (larger than ${request.maxSize}).")
      neighborhood
    } else {
      centers.toSet
    }

    val diagramMeta = {
      val op = graph_operations.SampledView(idSet)
      op(op.vertices, vertexSet)(op.ids, idAttr)(op.filtered, filtered).result.svVertices
    }
    val vertices = diagramMeta.value

    val attrs = request.attrs.map { attrId =>
      attrId -> {
        val attr = metaManager.attribute(attrId.asUUID)
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
    yBucketedAttr: graph_operations.BucketedAttribute[T],
    sampleSize: Int): Scalar[spark_util.IDBuckets[(Int, Int)]] = {

    val originalCount = graph_operations.Count.run(original)
    val op = graph_operations.VertexBucketGrid(xBucketedAttr.bucketer, yBucketedAttr.bucketer, sampleSize)
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
      val attribute = metaManager.attribute(request.xBucketingAttributeId.asUUID)
      dataManager.cache(attribute)
      FEBucketers.bucketedAttribute(
        metaManager, dataManager, attribute, request.xNumBuckets, request.xAxisOptions)
    } else {
      graph_operations.BucketedAttribute.emptyBucketedAttribute
    }
    val yBucketedAttr = if (request.yBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.attribute(request.yBucketingAttributeId.asUUID)
      dataManager.cache(attribute)
      FEBucketers.bucketedAttribute(
        metaManager, dataManager, attribute, request.yNumBuckets, request.yAxisOptions)
    } else {
      graph_operations.BucketedAttribute.emptyBucketedAttribute
    }

    val diagramMeta = getDiagramFromBucketedAttributes(
      vertexSet, filtered, xBucketedAttr, yBucketedAttr, request.sampleSize)
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
    for (id <- gUIDs) {
      dataManager.cache(metaManager.entity(id.asUUID))
    }
  }

  private def tripletMapping(
    eb: EdgeBundle, sampled: Boolean): graph_operations.TripletMapping.Output = {
    val op =
      if (sampled) graph_operations.TripletMapping(sampleSize = DrawingThresholds.TripletSampling)
      else graph_operations.TripletMapping()
    val res = op(op.edges, eb).result
    dataManager.cache(res.srcEdges)
    dataManager.cache(res.dstEdges)
    return res
  }

  private def edgesAndNeighborsMapping(
    eb: EdgeBundle, sampled: Boolean): graph_operations.EdgeAndNeighborMapping.Output = {
    val op =
      if (sampled) graph_operations.EdgeAndNeighborMapping(sampleSize = DrawingThresholds.TripletSampling)
      else graph_operations.EdgeAndNeighborMapping()
    val res = op(op.edges, eb).result
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

  private def mappedFilter[T](
    mapping: Attribute[Array[ID]],
    fa: graph_operations.FilteredAttribute[T],
    target: EdgeBundle): graph_operations.FilteredAttribute[T] = {
    val mattr = mappedAttribute(mapping, fa.attribute, target)
    graph_operations.FilteredAttribute(mattr, fa.filter)
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

  // Optionally returns a small set of edges filtered by srcView, dstView or both if specified.
  def getSmallEdgeSet(
    eb: EdgeBundle,
    srcView: graph_operations.VertexView,
    dstView: graph_operations.VertexView): Option[Seq[(ID, Edge)]] = {

    val mapping = edgesAndNeighborsMapping(eb, sampled = false)
    if (srcView.vertexIndices.isDefined) {
      val srcIds = srcView.vertexIndices.get.keySet
      val dstIds = dstView.vertexIndices.map(_.keySet)
      val op = graph_operations.EdgesForVerticesFromEdgesAndNeighbors(
        srcIds, dstIds, DrawingThresholds.SmallEdges, bySource = true)
      val edges =
        op(op.edges, eb)(op.mapping, mapping.srcEdges).result.edges.value
      if (edges.isDefined) return edges
    }
    if (dstView.vertexIndices.isDefined) {
      val srcIds = dstView.vertexIndices.get.keySet
      val dstIds = srcView.vertexIndices.map(_.keySet)
      val op = graph_operations.EdgesForVerticesFromEdgesAndNeighbors(
        srcIds, dstIds, DrawingThresholds.SmallEdges, bySource = false)
      val edges =
        op(op.edges, eb)(op.mapping, mapping.dstEdges).result.edges.value
      if (edges.isDefined) return edges
    }
    return None
  }

  def getAggregatedAttributeByCoord[From, To](
    ids: Set[ID],
    attributeWithAggregator: AttributeWithLocalAggregator[From, To],
    idToCoordMapping: Map[ID, (Int, Int)]): Map[(Int, Int), DynamicValue] = {

    val attrMap = graph_operations.RestrictAttributeToIds.run(
      attributeWithAggregator.attr, ids).value.toMap
    val byCoordMap = attributeWithAggregator.aggregator.aggregateByKey(
      attrMap.toSeq.flatMap { case (id, value) => idToCoordMapping.get(id).map(_ -> value) })
    implicit val ttT = attributeWithAggregator.aggregator.outputTypeTag(
      attributeWithAggregator.attr.typeTag)
    byCoordMap.mapValues(DynamicValue.convert[To](_))
  }

  def getEdgeDiagram(user: User, request: EdgeDiagramSpec): EdgeDiagramResponse = {
    val srcView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.srcDiagramId.asUUID))
    val dstView = graph_operations.VertexView.fromDiagram(
      metaManager.scalar(request.dstDiagramId.asUUID))
    val edgeBundle = metaManager.edgeBundle(request.edgeBundleId.asUUID)
    dataManager.cache(edgeBundle)
    val weights = if (request.edgeWeightId.isEmpty) {
      graph_operations.AddConstantAttribute.run(edgeBundle.idSet, 1.0)
    } else {
      val w = metaManager.attributeOf[Double](request.edgeWeightId.asUUID)
      dataManager.cache(w)
      w
    }
    assert(
      weights.vertexSet == edgeBundle.idSet,
      "The requested edge weight attribute does not belong to the requested edge bundle.\n" +
        s"Edge bundle: $edgeBundle\nWeight attribute: $weights")
    assert(srcView.vertexSet.gUID == edgeBundle.srcVertexSet.gUID,
      "Source vertex set does not match edge bundle source." +
        s"\nSource: ${srcView.vertexSet}\nEdge bundle source: ${edgeBundle.srcVertexSet}")
    assert(dstView.vertexSet.gUID == edgeBundle.dstVertexSet.gUID,
      "Destination vertex set does not match edge bundle destination." +
        s"\nSource: ${dstView.vertexSet}\nEdge bundle destination: ${edgeBundle.dstVertexSet}")

    val smallEdgeSetOption = getSmallEdgeSet(edgeBundle, srcView, dstView)
    val feEdges = smallEdgeSetOption match {
      case Some(smallEdgeSet) =>
        log.info("PERF Small edge set mode for request: " + request)
        val smallEdgeSetMap = smallEdgeSet.toMap
        val filteredEdgeSetIDs = FEFilters.localFilter(smallEdgeSetMap.keySet, request.filters)
        val filteredEdgeSet = filteredEdgeSetIDs.map(id => id -> smallEdgeSetMap(id))
        val srcIdxMapping = srcView.vertexIndices.getOrElse {
          val indexAttr = indexFromIndexingSeq(srcView.vertexSet, srcView.indexingSeq)
          val srcVertexIds = filteredEdgeSet.map { case (id, edge) => edge.src }.toSet
          graph_operations.RestrictAttributeToIds.run(indexAttr, srcVertexIds).value
        }
        val dstIdxMapping = dstView.vertexIndices.getOrElse {
          val indexAttr = indexFromIndexingSeq(dstView.vertexSet, dstView.indexingSeq)
          val dstVertexIds = filteredEdgeSet.map { case (id, edge) => edge.dst }.toSet
          graph_operations.RestrictAttributeToIds.run(indexAttr, dstVertexIds).value
        }
        val idToCoordMapping =
          filteredEdgeSet.flatMap {
            case (id, edge) =>
              val src = edge.src
              val dst = edge.dst
              if (srcIdxMapping.contains(src) && dstIdxMapping.contains(dst)) {
                Some(id -> (srcIdxMapping(src), dstIdxMapping(dst)))
              } else {
                None
              }
          }.toMap
        val attributesWithAggregators: Map[String, AttributeWithLocalAggregator[_, _]] =
          request.attrs.map(
            attr => (attr.attributeId + ":" + attr.aggregator) -> AttributeWithLocalAggregator(
              metaManager.attribute(attr.attributeId.asUUID),
              attr.aggregator)).toMap
        val attributeValues = attributesWithAggregators.mapValues(
          getAggregatedAttributeByCoord(filteredEdgeSetIDs, _, idToCoordMapping))
        val weightMap = graph_operations.RestrictAttributeToIds.run(
          weights, filteredEdgeSetIDs).value.toMap
        val counts = mutable.Map[(Int, Int), Double]().withDefaultValue(0.0)
        for ((id, coord) <- idToCoordMapping) {
          counts(coord) += weightMap(id)
        }
        counts
          .toMap
          .map {
            case ((s, d), c) =>
              FEEdge(
                s, d, c,
                attrs = attributeValues.mapValues(
                  _.getOrElse((s, d), DynamicValue(defined = false))))
          }
          .toSeq

      case None =>
        log.info("PERF Huge edge set mode for request: " + request)
        val edgeFilters = request.filters.map(_.toFilteredAttribute)
        val filtered = getFilteredEdgeIds(edgeBundle, srcView.filters, dstView.filters, edgeFilters)

        val srcIndices = edgeIndexFromIndexingSeq(
          edgeBundle, filtered.ids, filtered.srcTripletMapping, srcView.indexingSeq)
        val dstIndices = edgeIndexFromIndexingSeq(
          edgeBundle, filtered.ids, filtered.dstTripletMapping, dstView.indexingSeq)

        val originalEdgeCount = graph_operations.Count.run(edgeBundle)
        val countOp = graph_operations.IndexPairCounter()
        val counts = countOp(
          countOp.xIndices, srcIndices)(
            countOp.yIndices, dstIndices)(
              countOp.original, edgeBundle.idSet)(
                countOp.weights, weights)(
                  countOp.originalCount, originalEdgeCount).result.counts.value
        counts.map { case ((s, d), c) => FEEdge(s, d, c) }.toSeq
    }
    log.info("PERF edge counts computed")
    EdgeDiagramResponse(
      request.srcDiagramId,
      request.dstDiagramId,
      request.srcIdx,
      request.dstIdx,
      feEdges,
      if (request.layout3D) ForceLayout3D(feEdges) else Map())
  }

  // Recalculates the edge weights to be relative to the vertex sizes.
  def relativeEdgeDensity(
    ed: EdgeDiagramResponse, src: VertexDiagramResponse, dst: VertexDiagramResponse): EdgeDiagramResponse = {
    val originalEdges = ed.edges
    val newEdges = originalEdges.map { feEdge =>
      val srcSize = src.vertices(feEdge.a).size
      val dstSize = dst.vertices(feEdge.b).size
      if (srcSize > 0 && dstSize > 0)
        feEdge.copy(size = feEdge.size / (srcSize * dstSize))
      else
        feEdge.copy(size = 0)
    }
    ed.copy(edges = newEdges)
  }

  def getComplexView(user: User, request: FEGraphRequest): FEGraphResponse = {
    val vertexDiagrams = request.vertexSets.map(getVertexDiagram(user, _))
    for ((spec, diag) <- request.vertexSets zip vertexDiagrams) {
      assert(
        diag.size <= spec.maxSize,
        s"Vertex diagram too large to display. Would return ${diag.size} vertices for $spec")
    }
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
    val edgeDiagrams = modifiedEdgeSpecs.map { spec =>
      val ed = getEdgeDiagram(user, spec)
      if (spec.relativeEdgeDensity) {
        val src = vertexDiagrams.find(diag => diag.diagramId == spec.srcDiagramId).get
        val dst = vertexDiagrams.find(diag => diag.diagramId == spec.dstDiagramId).get
        relativeEdgeDensity(ed, src, dst)
      } else ed
    }
    for ((spec, diag) <- request.edgeBundles zip edgeDiagrams) {
      assert(
        diag.size <= spec.maxSize,
        s"Edge diagram too large to display. Would return ${diag.size} edges for $spec")
    }
    spark_util.Counters.printAll
    FEGraphResponse(vertexDiagrams, edgeDiagrams)
  }

  private def getFilteredVSByFA(
    vertexSet: VertexSet,
    vertexFilters: Seq[graph_operations.FilteredAttribute[_]]): VertexSet = {
    loadGUIDsToMemory(vertexFilters.map(_.attribute.gUID.toString))
    FEFilters.filterFA(vertexSet, vertexFilters)
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
    dstFilters: Seq[graph_operations.FilteredAttribute[_]],
    edgeFilters: Seq[graph_operations.FilteredAttribute[_]]): FilteredEdges = {
    val sampledTrips = tripletMapping(edgeBundle, sampled = true)
    val sampledEdges = getFilteredEdgeIds(sampledTrips, edgeBundle, srcFilters, dstFilters, edgeFilters)
    // TODO: See if we can eliminate the extra stage from this "count".
    val count = graph_operations.Count.run(sampledEdges.ids).value
    if (count >= DrawingThresholds.FilterMinRemaining) {
      sampledEdges
    } else {
      // Too little remains of the sample after filtering. Let's filter the full data.
      val fullTrips = tripletMapping(edgeBundle, sampled = false)
      getFilteredEdgeIds(fullTrips, edgeBundle, srcFilters, dstFilters, edgeFilters)
    }
  }

  private def getFilteredEdgeIds(
    trips: graph_operations.TripletMapping.Output,
    edgeBundle: EdgeBundle,
    srcFilters: Seq[graph_operations.FilteredAttribute[_]],
    dstFilters: Seq[graph_operations.FilteredAttribute[_]],
    edgeFilters: Seq[graph_operations.FilteredAttribute[_]]): FilteredEdges = {
    val srcMapped = srcFilters.map(mappedFilter(trips.srcEdges, _, edgeBundle))
    val dstMapped = dstFilters.map(mappedFilter(trips.dstEdges, _, edgeBundle))
    val ids = getFilteredVSByFA(edgeBundle.idSet, srcMapped ++ dstMapped ++ edgeFilters)
    FilteredEdges(ids, trips.srcEdges, trips.dstEdges)
  }

  def getCenter(user: User, request: CenterRequest): Future[CenterResponse] = {
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    dataManager.cache(vertexSet)
    loadGUIDsToMemory(request.filters.map(_.attributeId))
    val filtered = FEFilters.filter(vertexSet, request.filters)
    val sampled = {
      val op = graph_operations.SampleVertices(request.count)
      op(op.vs, filtered).result.sample
    }
    dataManager
      .getFuture(sampled)
      .map(s => CenterResponse(s.value.map(_.toString)))
      .future
  }

  def getHistogram(user: User, request: HistogramSpec): Future[HistogramResponse] = {
    val attribute = metaManager.attribute(request.attributeId.asUUID)
    dataManager.cache(attribute.vertexSet)
    dataManager.cache(attribute)
    loadGUIDsToMemory(request.vertexFilters.map(_.attributeId))
    val bucketedAttr = FEBucketers.bucketedAttribute(
      metaManager, dataManager, attribute, request.numBuckets, request.axisOptions)
    val filteredVS = if (request.edgeBundleId.isEmpty) {
      getFilteredVS(attribute.vertexSet, request.vertexFilters)
    } else {
      val edgeBundle = metaManager.edgeBundle(request.edgeBundleId.asUUID)
      val vertexFilters = request.vertexFilters.map(_.toFilteredAttribute)
      val edgeFilters = request.edgeFilters.map(_.toFilteredAttribute)
      getFilteredEdgeIds(edgeBundle, vertexFilters, vertexFilters, edgeFilters).ids
    }
    val histogram = bucketedAttr.toHistogram(filteredVS, request.sampleSize)
    val counts = histogram.counts
    spark_util.Counters.printAll
    dataManager
      .getFuture(counts)
      .map(c =>
        HistogramResponse(
          bucketedAttr.bucketer.labelType,
          bucketedAttr.bucketer.bucketLabels,
          (0 until bucketedAttr.bucketer.numBuckets).map(c.value.getOrElse(_, 0L))))
      .future
  }

  def getScalarValue(user: User, request: ScalarValueRequest): Future[DynamicValue] = {
    val scalar = metaManager.scalar(request.scalarId.asUUID)
    dataManager
      .getFuture(scalar)
      .map(dynamicValue(_)).future
  }

  def getModel(user: User, request: ScalarValueRequest): Future[model.FEModel] = {
    val m = metaManager.scalar(request.scalarId.asUUID).runtimeSafeCast[model.Model]
    dataManager
      .getFuture(m)
      .map(scalar => model.Model.toFE(scalar.value, dataManager.runtimeContext.sparkContext))
      .future
  }

  private def dynamicValue[T](scalar: ScalarData[T]) = {
    implicit val tt = scalar.typeTag
    graph_operations.DynamicValue.convert(scalar.value)
  }
}
