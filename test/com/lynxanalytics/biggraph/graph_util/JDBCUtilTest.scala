package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

class JDBCUtilTest extends FunSuite {
  test("String partitioning: generic case") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak", "zsiraf", 5)
    assert(r == Seq(
      "c < \"ff9qVjOnbB\"",
      "\"ff9qVjOnbB\" <= c AND c < \"kiY6HSnbCF\"",
      "\"kiY6HSnbCF\" <= c AND c < \"plwM3CCOnI\"",
      "\"plwM3CCOnI\" <= c AND c < \"upKbovbCOL\"",
      "\"upKbovbCOL\" <= c"))
  }

  test("String partitioning: single partition") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak", "zsiraf", 1)
    assert(r == Seq(null))
  }

  test("String partitioning: two partitions") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak", "zsiraf", 2)
    assert(r == Seq(
      "c < \"nFFEAKV0z1\"",
      "\"nFFEAKV0z1\" <= c"
    ))
  }

  test("String partitioning: tight range") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak1", "ablak2", 3)
    assert(r == Seq(
      "c < \"ablak1KfLD\"",
      "\"ablak1KfLD\" <= c AND c < \"ablak1fKgz\"",
      "\"ablak1fKgz\" <= c"
    ))
  }

  test("String partitioning: Unicode characters") {
    // High Unicode characters like the snowman are effectively just replaced with 'z'.
    // This is acceptable generally, but will fail in some cases.
    intercept[AssertionError] { JDBCUtil.stringPartitionClauses("c", "zsiraf", "☃ snowman", 3) }
    intercept[AssertionError] { JDBCUtil.stringPartitionClauses("c", "☃ snowman", "❄ snow", 3) }
    intercept[AssertionError] { JDBCUtil.stringPartitionClauses("c", "❄ snow", "☃ snowman", 3) }
    val r = JDBCUtil.stringPartitionClauses("c", "ablak", "☃ snowman", 3)
    assert(r == Seq(
      "c < \"ik8f6JaraE\"",
      "\"ik8f6JaraE\" <= c AND c < \"qsVjSdBjCp\"",
      "\"qsVjSdBjCp\" <= c"))
  }
}
