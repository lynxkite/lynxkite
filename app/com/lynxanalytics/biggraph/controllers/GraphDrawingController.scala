package com.lynxanalytics.biggraph.controllers

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
  val yBucketingAttributeId: String = "",

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

