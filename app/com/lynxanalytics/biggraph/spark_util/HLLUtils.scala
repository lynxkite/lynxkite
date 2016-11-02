// Utility class for HLLs.
package com.lynxanalytics.biggraph.spark_util

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus

case class HLLUtils(bits: Int) {
  def hllFromObject(obj: Any): HyperLogLogPlus = {
    val hll = new HyperLogLogPlus(bits)
    hll.offer(obj)
    hll
  }

  def union(hll1: HyperLogLogPlus, hll2: HyperLogLogPlus): HyperLogLogPlus = {
    val hll3 = new HyperLogLogPlus(bits)
    hll3.addAll(hll1)
    hll3.addAll(hll2)
    hll3
  }

  def union(hll1: Option[HyperLogLogPlus], hll2: Option[HyperLogLogPlus]): HyperLogLogPlus = {
    val hll3 = new HyperLogLogPlus(bits)
    for (hll <- hll1 ++ hll2) hll3.addAll(hll)
    hll3
  }

  def intersectSize(hll1: HyperLogLogPlus, hll2: HyperLogLogPlus): Long = {
    val c1 = hll1.cardinality
    val c2 = hll2.cardinality
    val c12 = union(hll1, hll2).cardinality
    (c1 + c2 - c12) max 0
  }
}

