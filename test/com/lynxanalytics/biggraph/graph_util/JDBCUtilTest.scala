package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

class JDBCUtilTest extends FunSuite {
  test("String partitioning: generic case") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak", "zsiraf", 5)
    assert(r == Seq(
      "c < \"ff9EujBapm\"",
      "\"ff9EujBapm\" <= c AND c < \"kiXu4SOBcr\"",
      "\"kiXu4SOBcr\" <= c AND c < \"plwYFBanNC\"",
      "\"plwYFBanNC\" <= c AND c < \"upKCPvnOAH\"",
      "\"upKCPvnOAH\" <= c"))
  }

  test("String partitioning: single partition") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak", "zsiraf", 1)
    assert(r == Seq(null))
  }

  test("String partitioning: two partitions") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak", "zsiraf", 2)
    assert(r == Seq(
      "c < \"nEkDfK  0t\"",
      "\"nEkDfK  0t\" <= c"
    ))
  }

  test("String partitioning: tight range") {
    val r = JDBCUtil.stringPartitionClauses("c", "ablak1", "ablak2", 3)
    assert(r == Seq(
      "c < \"ablak1K 02\"",
      "\"ablak1K 02\" <= c AND c < \"ablak1f 1B\"",
      "\"ablak1f 1B\" <= c"
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
      "c < \"ik8 5dvWdS\"",
      "\"ik8 5dvWdS\" <= c AND c < \"qsVOSHr3DD\"",
      "\"qsVOSHr3DD\" <= c"))
  }
}
