package com.lynxanalytics.lynxkite.graph_util

import org.scalatest.funsuite.AnyFunSuite

class VertexBucketerTest extends AnyFunSuite {
  test("Long bucketer works as expected") {
    assert((1 to 6).flatMap(LongBucketer(1, 6, 3).whichBucket(_)) ==
      Seq(0, 0, 1, 1, 2, 2))
    assert((1 to 7).flatMap(LongBucketer(1, 7, 3).whichBucket(_)) ==
      Seq(0, 0, 0, 1, 1, 1, 2))
    assert((1 to 8).flatMap(LongBucketer(1, 8, 3).whichBucket(_)) ==
      Seq(0, 0, 0, 1, 1, 1, 2, 2))
    assert((1 to 9).flatMap(LongBucketer(1, 9, 3).whichBucket(_)) ==
      Seq(0, 0, 0, 1, 1, 1, 2, 2, 2))
    assert(new LongBucketer(1, 6, 3).bounds == Seq(3, 5))
  }

  test("Double linear bucketer works as expected") {
    var fb = DoubleLinearBucketer(1, 6, 3)
    assert(fb.bounds == Seq(1 + 5.0 / 3, 1 + 2 * 5.0 / 3))
    assert(fb.whichBucket(1.00).get == 0)
    assert(fb.whichBucket(2.66).get == 0)
    assert(fb.whichBucket(2.67).get == 1)
    assert(fb.whichBucket(4.33).get == 1)
    assert(fb.whichBucket(4.34).get == 2)
    assert(fb.whichBucket(6.00).get == 2)
    fb = DoubleLinearBucketer(0.2, 0.9, 7)
    assert(fb.bounds == Seq(0.3, 0.4, 0.5, 0.6, 0.7, 0.8))
  }

  test("Double logarithmic bucketer with all kinds of values") {
    var fb = DoubleLogBucketer(-1, 1000, Some(1), 3)
    // This is not exactly true, thanks to inaccuracies in the arithmetic.
    // assert(fb.bounds == Seq(1, 10, 100))
    // But when rounded for string formatting, we get the expected result.
    assert(fb.bucketLabels == Seq("-1.0", "1.0", "10.0", "100.0", "1000.0"))
    // And numbers generally end up in the right bucket.
    assert(fb.whichBucket(-1).get == 0)
    assert(fb.whichBucket(0).get == 0)
    assert(fb.whichBucket(1).get == 1)
    assert(fb.whichBucket(9).get == 1)
    assert(fb.whichBucket(10).get == 2)
    assert(fb.whichBucket(99).get == 2)
    assert(fb.whichBucket(100).get == 3)
    assert(fb.whichBucket(1000).get == 3)
  }

  test("Double logarithmic bucketer with no positive values") {
    var fb = DoubleLogBucketer(-1, 0, None, 3)
    assert(fb.bucketLabels == Seq("-1.0", "0.0"))
    assert(fb.whichBucket(-1).get == 0)
    assert(fb.whichBucket(0).get == 0)
    assert(fb.whichBucket(1).get == 0)
  }

  test("Double logarithmic bucketer with no non-positive values") {
    var fb = DoubleLogBucketer(1, 1000, Some(1), 3)
    assert(fb.bucketLabels == Seq("1.0", "10.0", "100.0", "1000.0"))
    assert(fb.whichBucket(-1).get == 0)
    assert(fb.whichBucket(0).get == 0)
    assert(fb.whichBucket(1).get == 0)
    assert(fb.whichBucket(11).get == 1)
    assert(fb.whichBucket(111).get == 2)
    assert(fb.whichBucket(1111).get == 2)
  }

  test("Unexpected strings are ignored") {
    val b = StringBucketer(Seq("adam", "eve"), hasOther = false)
    assert(b.whichBucket("adam").get == 0)
    assert(b.whichBucket("eve").get == 1)
    assert(b.whichBucket("george") == None)
  }

  test("Bucketing numeric labels are wonderful") {
    assert(LongBucketer(1, 8, 3).bucketLabels == Seq("1", "4", "7", "8"))
  }

  test("Bucketing double labels for large numbers") {
    assert(
      DoubleLinearBucketer(100, 400, 3).bucketLabels == Seq("100.0", "200.0", "300.0", "400.0"))
  }
  test("Bucketing double labels for small numbers") {
    assert(
      DoubleLinearBucketer(0.001, 0.002, 2).bucketLabels == Seq("0.00100", "0.00150", "0.00200"))
  }
  test("Bucketing double labels for small differences") {
    assert(DoubleLinearBucketer(3.001, 3.004, 3).bucketLabels ==
      Seq("3.0010", "3.0020", "3.0030", "3.0040"))
  }

  test("Bucketing long labels by integer division") {
    assert(LongBucketer(0, 4, 5).bucketLabels == Seq("0", "1", "2", "3", "4"))
  }

  test("Bucketing string labels are wonderful too") {
    assert(StringBucketer(Seq("adam", "eve", "george"), hasOther = true).bucketLabels
      == Seq("adam", "eve", "george", "Other"))
  }

  test("String bucket filters") {
    assert(StringBucketer(Seq("adam", "eve"), hasOther = false).bucketFilters
      == Seq("adam", "eve"))
    assert(StringBucketer(Seq("adam", "eve"), hasOther = true).bucketFilters
      == Seq("adam", "eve", "!adam,eve"))
  }

  test("Double bucket filters") {
    assert(DoubleLinearBucketer(10, 20, 1).bucketFilters
      == Seq(""))
    assert(DoubleLinearBucketer(10, 30, 2).bucketFilters
      == Seq("<20.0", ">=20.0"))
    assert(DoubleLinearBucketer(10, 40, 3).bucketFilters
      == Seq("<20.0", "[20.0,30.0)", ">=30.0"))
    assert(DoubleLogBucketer(1, 10000, Some(1), 4).bucketFilters
      == Seq(
        "<10.000000000000002",
        "[10.000000000000002,100.00000000000004)",
        "[100.00000000000004,1000.0000000000007)",
        ">=1000.0000000000007"))
  }
}
