package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.apache.spark
import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_util

class ReadParquetWithSchemaTest extends AnyFunSuite with TestGraphOp {
  def assertRoughlyEqual(x: Double, y: Double) = {
    assert(x >= y - 0.01)
    assert(x <= y + 0.01)
  }
  def assertRoughlyEqualDecimal(x: BigDecimal, y: Double) = {
    assert(x >= y - 0.01)
    assert(x <= y + 0.01)
  }
  test("test table with many types") {
    val resourceDir = getClass.getResource("/graph_operations").toString
    graph_util.PrefixRepository.registerPrefix("GRAPHOPERATIONS$", resourceDir)
    val t = ReadParquetWithSchema.run(
      "GRAPHOPERATIONS$/many-types.parquet",
      """
      id: long
      string_col: string
      double_col: double
      decimal_col: decimal(18,2)
      date_col: date
      time_col: timestamp
      array_col: array of double
      """.trim.split("\n").map(_.trim),
    )
    val df = t.df
    assert(
      df.schema.map(_.name).mkString(" ") ==
        "id string_col double_col decimal_col date_col time_col array_col")
    val r = df.head
    assert(r.getLong(0) == 5)
    assert(r.getString(1) == "0.018906521631430362")
    assertRoughlyEqual(r.getDouble(2), 0.67)
    assertRoughlyEqualDecimal(r.getDecimal(3), 0.67)
    assert(r.getDate(4) == java.sql.Date.valueOf("2022-02-15"))
    assert(r.getTimestamp(5) == java.sql.Timestamp.valueOf("2022-02-22 22:18:00"))
    val arr = r.getSeq[Double](6)
    assert(arr.length == 3)
    assertRoughlyEqualDecimal(arr.head, 0.17)
  }
}
