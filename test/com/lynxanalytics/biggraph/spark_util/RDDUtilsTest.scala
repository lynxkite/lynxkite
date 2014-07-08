package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite
import org.apache.spark.HashPartitioner
import com.lynxanalytics.biggraph.TestSparkContext

class RDDUtilsTest extends FunSuite with TestSparkContext {
  test("fastNumbered works for a few items") {
    import RDDUtils.Implicit
    val rdd = sparkContext.parallelize(1 to 10, 3)
    val numbered = rdd.fastNumbered.collect.toSeq
    assert(numbered == Seq(0 -> 1, 3 -> 2, 6 -> 3, 1 -> 4, 4 -> 5, 7 -> 6, 2 -> 7, 5 -> 8, 8 -> 9, 11 -> 10))
  }

  test("genID works for small and large numbers") {
    val parts = 10
    val border = Int.MaxValue / parts
    val interestingRows = Seq(
      0, 1, parts - 1, parts, parts + 1,
      border - 1, border, border + 1,
      2 * border - 1, 2 * border, 2 * border + 1,
      3 * border - 1, 3 * border, 3 * border + 1,
      4 * border - 1, 4 * border, 4 * border + 1,
      5 * border - 1, 5 * border, 5 * border + 1)
    val partitioner = new HashPartitioner(parts)
    val ids = collection.mutable.Set[Long]()
    for (part <- 0 until parts) {
      for (row <- interestingRows) {
        val id = RDDUtils.genID(parts, part, row)
        assert(partitioner.getPartition(id) == part, s"genID($parts, $part, $row)")
        assert(!ids.contains(id), s"genID($parts, $part, $row)")
        ids += id
      }
    }
  }
}
