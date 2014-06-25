package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.ConcatenateBundles

class BundleChain(weights: Seq[EdgeAttribute[Double]]) {
  val bundles = weights.map(_.edgeBundle)
  assert(bundles.size > 0)
  assert((0 until (bundles.size - 1))
    .forall(i => bundles(i).dstVertexSet == bundles(i + 1).srcVertexSet))
  val vertexSets = bundles.head.srcVertexSet +: bundles.map(_.dstVertexSet)

  def getCompositeEdgeBundle(metaManager: MetaGraphManager): EdgeAttribute[Double] = {
    if (weights.size == 1) {
      weights.head
    } else {
      val splitterIdx = vertexSets
        .zipWithIndex
        .slice(1, vertexSets.size - 1)
        .maxBy { case (vertexSet, idx) => vertexSet.gUID.toString }
        ._2
      val firstWeights =
        (new BundleChain(weights.slice(0, splitterIdx))).getCompositeEdgeBundle(metaManager)
      val secondWeights =
        (new BundleChain(weights.drop(splitterIdx))).getCompositeEdgeBundle(metaManager)
      val inst = metaManager.apply(
        ConcatenateBundles(),
        'weightsAB -> firstWeights,
        'weightsBC -> secondWeights)
      inst.outputs.edgeAttributes('weightsAC).runtimeSafeCast[Double]
    }
  }
}
