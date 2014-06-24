package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_operations

case class VertexAttributeFilter(
  val attributeId: String,
  val valueSpec: String)
case class VertexDiagramSpec(
  val vertexSetId: String,
  val filters: Seq[VertexAttributeFilter],
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
  count: Int)
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
  val bundleIdSequence: Seq[String])

case class FEEdge(
  // idx of source vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  a: Int,
  // idx of destination vertex in the vertices Seq in the corresponding VertexDiagramResponse.
  b: Int,
  count: Int)

case class EdgeDiagramResponse(
  val srcDiagramId: String,
  val dstDiagramId: String,
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

  def getVertexDiagram(request: VertexDiagramSpec): VertexDiagramResponse = {
    if (request.mode != "bucketed") return ???
    val vertexSet = metaManager.vertexSet(request.vertexSetId.asUUID)
    val count = graph_operations.CountVertices(metaManager, dataManager, vertexSet)
    // TODO get from request or something.
    val targetSample = 10000
    val sampled =
      if (count <= targetSample) vertexSet
      else metaManager.apply(
        graph_operations.VertexSample(targetSample * 1.0 / count),
        'vertices -> vertexSet).outputs.vertexSets('sampled)

    var (xMin, xMax, yMin, yMax) = (-1.0, -1.0, -1.0, -1.0)
    var inputs = MetaDataSet()
    if (request.xNumBuckets > 1 && request.xBucketingAttributeId.nonEmpty) {
      val attribute =
        metaManager.vertexAttribute(request.xBucketingAttributeId.asUUID).runtimeSafeCast[Double]
      val (min, max) = graph_operations.ComputeMinMax(metaManager, dataManager, attribute)
      xMin = min
      xMax = max
      inputs ++= MetaDataSet(Map('xAttribute -> attribute))
    }
    if (request.yNumBuckets > 1 && request.yBucketingAttributeId.nonEmpty) {
      val attribute =
        metaManager.vertexAttribute(request.yBucketingAttributeId.asUUID).runtimeSafeCast[Double]
      val (min, max) = graph_operations.ComputeMinMax(metaManager, dataManager, attribute)
      yMin = min
      yMax = max
      inputs ++= MetaDataSet(Map('yAttribute -> attribute))
    }
    val op = graph_operations.VertexBucketGrid(
      request.xNumBuckets, request.yNumBuckets, xMin, xMax, yMin, yMax)
    val diagramMeta = metaManager.apply(op, inputs)
      .outputs.scalars('bucketSizes).runtimeSafeCast[Map[(Int, Int), Int]]
    val diagram = dataManager.get(diagramMeta).value

    val vertices = for (x <- (0 to request.xNumBuckets); y <- (0 to request.yNumBuckets))
      yield FEVertex(x, y, diagram.getOrElse((x, y), 0))

    VertexDiagramResponse(
      diagramId = diagramMeta.gUID.toString,
      vertices = vertices,
      mode = "bucketed",
      xBuckets = op.xBucketLabels,
      yBuckets = op.yBucketLabels)
  }

  def getEdgeDiagram(request: EdgeDiagramSpec): EdgeDiagramResponse =
    EdgeDiagramResponse(
      request.srcDiagramId,
      request.dstDiagramId,
      Seq(FEEdge(0, 0, 10)))

  def getComplexView(request: FEGraphRequest): FEGraphRespone = {
    val vertexDiagrams = request.vertexSets.map(getVertexDiagram(_))
    val idxPattern = "idx\\[\\(d+)\\]".r
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
