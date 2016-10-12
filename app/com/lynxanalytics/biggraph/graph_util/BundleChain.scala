// Concatenates a series of edge bundles.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.ConcatenateBundles
import com.lynxanalytics.biggraph.graph_operations.AddConstantAttribute

class BundleChain(bundles: Seq[EdgeBundle],
                  weightsParam: Option[Seq[Attribute[Double]]] = None) {

  assert(bundles.nonEmpty, "Cannot chain an empty list of edge bundles.")
  for (weightsSeq <- weightsParam) {
    assert(weightsSeq.size == bundles.size,
      s"Got ${weightsSeq.size} weights for ${bundles.size} bundles.")
    for ((bundle, weight) <- bundles.zip(weightsSeq)) {
      assert(bundle.idSet == weight.vertexSet,
        s"$weight is for ${weight.vertexSet} not for $bundle's idSet.")
    }
  }
  val weights = weightsParam
    .getOrElse(bundles.map(bundle => AddConstantAttribute.run(bundle.idSet, 1.0)))

  for (i <- 0 until (bundles.size - 1)) {
    assert(bundles(i).dstVertexSet == bundles(i + 1).srcVertexSet,
      s"The destination of bundles($i) is not the source of bundles(${i + 1}).")
  }
  val vertexSets = bundles.head.srcVertexSet +: bundles.map(_.dstVertexSet)

  def getCompositeEdgeBundle: (EdgeBundle, Attribute[Double]) = {
    implicit val metaManager = bundles.head.manager
    if (bundles.size == 1) {
      (bundles.head, weights.head)
    } else {
      val splitterIdx = vertexSets
        .zipWithIndex
        .slice(1, vertexSets.size - 1)
        .maxBy { case (vertexSet, idx) => vertexSet.gUID.toString }
        ._2
      val (firstBundle, firstWeights) =
        (new BundleChain(bundles.slice(0, splitterIdx), Some(weights.slice(0, splitterIdx))))
          .getCompositeEdgeBundle
      val (secondBundle, secondWeights) =
        (new BundleChain(bundles.drop(splitterIdx), Some(weights.drop(splitterIdx))))
          .getCompositeEdgeBundle
      import com.lynxanalytics.biggraph.graph_api.Scripting._
      val op = ConcatenateBundles()
      val res = op(
        op.edgesAB, firstBundle)(
          op.edgesBC, secondBundle)(
            op.weightsAB, firstWeights)(
              op.weightsBC, secondWeights).result
      (res.edgesAC, res.weightsAC)
    }
  }
}
