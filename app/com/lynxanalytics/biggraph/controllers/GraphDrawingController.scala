package com.lynxanalytics.biggraph.controllers

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util

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
  val radius: Int = 1)

case class FEVertex(
  x: Int,
  y: Int,
  size: Int)
case class VertexDiagramResponse(
  val diagramId: String,
  val vertices: Seq[FEVertex],
  val mode: String, // as specified in the request

  // ** Only set for bucketed view **
  val xBuckets: Seq[String] = Seq(),
  val yBuckets: Seq[String] = Seq())

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
  val metaManager = env.metaGraphManager
  val dataManager = env.dataManager

  import graph_operations.SampledVertexAttribute.sampleAttribute

  def filter(sampled: VertexSet, filters: Seq[FEVertexAttributeFilter]): VertexSet = {
    if (filters.isEmpty) return sampled
    val filteredVss = filters.map { filterSpec =>
      FEFilters.filteredBaseSet(
        metaManager,
        metaManager.vertexAttribute(filterSpec.attributeId.asUUID),
        filterSpec.valueSpec)
    }
    return graph_operations.VertexSetIntersection.intersect(metaManager, filteredVss: _*)
  }

  def getVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    if (request.mode != "bucketed") return ???
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    val count = graph_operations.CountVertices(metaManager, dataManager, vertexSet)
    // TODO get from request or something.
    val targetSample = 10000
    val fraction = (targetSample * 1.0 / count) min 1.0
    val sampled =
      if (fraction < 1.0) metaManager.apply(
        graph_operations.VertexSample(fraction),
        'vertices -> vertexSet).outputs.vertexSets('sampled)
      else vertexSet

    val filtered = filter(sampled, request.filters)

    var xBucketer: graph_util.Bucketer[_] = graph_util.EmptyBucketer()
    var yBucketer: graph_util.Bucketer[_] = graph_util.EmptyBucketer()
    var inputs = MetaDataSet(Map('vertices -> filtered))
    if (request.xNumBuckets > 1 && request.xBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.xBucketingAttributeId.asUUID)
      val sampledAttribute = sampleAttribute(
        metaManager, filtered, sampleAttribute(metaManager, sampled, attribute))
      xBucketer = FEBucketers.bucketer(
        metaManager, dataManager, sampledAttribute, request.xNumBuckets)
      if (xBucketer.numBuckets > 1) {
        inputs ++= MetaDataSet(
          Map('xAttribute -> sampledAttribute))
      }
    }
    if (request.yNumBuckets > 1 && request.yBucketingAttributeId.nonEmpty) {
      val attribute = metaManager.vertexAttribute(request.yBucketingAttributeId.asUUID)
      val sampledAttribute = sampleAttribute(
        metaManager, filtered, sampleAttribute(metaManager, sampled, attribute))
      yBucketer = FEBucketers.bucketer(
        metaManager, dataManager, sampledAttribute, request.yNumBuckets)
      if (yBucketer.numBuckets > 1) {
        inputs ++= MetaDataSet(
          Map('yAttribute -> sampledAttribute))
      }
    }
    val op = graph_operations.VertexBucketGrid(xBucketer, yBucketer)
    val diagramMeta = metaManager.apply(op, inputs)
      .outputs.scalars('bucketSizes).runtimeSafeCast[Map[(Int, Int), Int]]
    val diagram = dataManager.get(diagramMeta).value

    val vertices = for (x <- (0 until xBucketer.numBuckets); y <- (0 until yBucketer.numBuckets))
      yield FEVertex(x, y, (diagram.getOrElse((x, y), 0) * 1.0 / fraction).toInt)

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices,
      mode = "bucketed",
      xBuckets = xBucketer.bucketLabels,
      yBuckets = yBucketer.bucketLabels)
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
    inst.inputs.vertexSets('vertices)
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
      'input -> eb,
      'srcSubset -> src,
      'dstSubset -> dst).outputs.edgeBundles('induced)

  private def tripletMapping(eb: EdgeBundle): (VertexAttribute[Array[ID]], VertexAttribute[Array[ID]]) = {
    val metaOut = metaManager.apply(
      graph_operations.TripletMapping(),
      'input -> eb).outputs
    return (
      metaOut.vertexAttributes('srcEdges).runtimeSafeCast[Array[ID]],
      metaOut.vertexAttributes('dstEdges).runtimeSafeCast[Array[ID]])
  }

  private def idxsFromInst(inst: MetaGraphOperationInstance): VertexAttribute[Int] =
    inst.outputs.vertexAttributes('gridIdxs).runtimeSafeCast[Int]

  private def numYBuckets(inst: MetaGraphOperationInstance): Int = {
    inst.operation.asInstanceOf[graph_operations.VertexBucketGrid[_, _]].yBucketer.numBuckets
  }

  private def mappedAttribute(mapping: VertexAttribute[Array[ID]],
                              attr: VertexAttribute[Int],
                              target: EdgeBundle): EdgeAttribute[Int] =
    metaManager.apply(
      new graph_operations.VertexToEdgeIntAttribute(),
      'mapping -> mapping,
      'original -> attr,
      'target -> target).outputs.edgeAttributes('mapped_attribute).runtimeSafeCast[Int]

  def getEdgeDiagram(request: EdgeDiagramSpec): EdgeDiagramResponse = {
    val srcOp = metaManager.scalar(request.srcDiagramId.asUUID).source
    val dstOp = metaManager.scalar(request.dstDiagramId.asUUID).source
    val bundleWeights = getCompositeBundle(request.bundleSequence)
    val induced = inducedBundle(bundleWeights.edgeBundle, vsFromOp(srcOp), vsFromOp(dstOp))
    val (srcMapping, dstMapping) = tripletMapping(induced)
    val srcIdxs = mappedAttribute(
      sampleAttribute(metaManager, directVsFromOp(srcOp), srcMapping),
      idxsFromInst(srcOp),
      induced)
    val dstIdxs = mappedAttribute(
      sampleAttribute(metaManager, directVsFromOp(dstOp), dstMapping),
      idxsFromInst(dstOp),
      induced)
    val srcIdxsRDD = dataManager.get(srcIdxs).rdd
    val dstIdxsRDD = dataManager.get(dstIdxs).rdd
    val idxPairBuckets = srcIdxsRDD.join(dstIdxsRDD)
      .map { case (eid, (s, d)) => ((s, d), 1) }
      .reduceByKey(_ + _)
      .collect
    EdgeDiagramResponse(
      request.srcDiagramId,
      request.dstDiagramId,
      request.srcIdx,
      request.dstIdx,
      idxPairBuckets.map { case ((s, d), c) => FEEdge(s, d, c) })
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
