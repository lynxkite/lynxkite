package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

class VertexBucketerTest extends FunSuite {
  test("Long bucketer works as expected") {
    assert((1 to 6).map(new NumericBucketer[Long](1, 6, 3).whichBucket(_)) ==
      Seq(0, 0, 1, 1, 2, 2))
    assert((1 to 7).map(new NumericBucketer[Long](1, 7, 3).whichBucket(_)) ==
      Seq(0, 0, 0, 1, 1, 1, 2))
    assert((1 to 8).map(new NumericBucketer[Long](1, 8, 3).whichBucket(_)) ==
      Seq(0, 0, 0, 1, 1, 1, 2, 2))
    assert((1 to 9).map(new NumericBucketer[Long](1, 9, 3).whichBucket(_)) ==
      Seq(0, 0, 0, 1, 1, 1, 2, 2, 2))
    assert(new NumericBucketer[Long](1, 6, 3).bounds == Seq(3, 5))
  }
  test("Double bucketer works as expected") {
    var fb = new FractionalBucketer[Double](1, 6, 3)
    assert(fb.bounds == Seq(1 + 5.0 / 3, 1 + 2 * 5.0 / 3))
    assert(fb.whichBucket(1.00) == 0)
    assert(fb.whichBucket(2.66) == 0)
    assert(fb.whichBucket(2.67) == 1)
    assert(fb.whichBucket(4.33) == 1)
    assert(fb.whichBucket(4.34) == 2)
    assert(fb.whichBucket(6.00) == 2)
    fb = new FractionalBucketer[Double](0.2, 0.9, 7)
    assert(fb.bounds == Seq(0.3, 0.4, 0.5, 0.6, 0.7, 0.8))
  }
  test("Bucketing labels are wonderful") {
    assert(NumericBucketer.bucketLabels(new NumericBucketer[Long](1, 8, 3)) ==
             Seq("[1, 4)", "[4, 7)", "[7, 8]"))
  }
}
