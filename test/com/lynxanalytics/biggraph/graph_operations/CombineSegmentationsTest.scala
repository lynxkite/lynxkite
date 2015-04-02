package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util._

class CombineSegmentationsTest extends FunSuite with TestGraphOp {
  test("example graph by gender and name") {
    val g = ExampleGraph()().result
    val sb = StringBucketing()
    val byGender = sb(sb.attr, g.gender).result
    assert(byGender.segments.toSeq.size == 2)

    val byName = sb(sb.attr, g.name).result
    assert(byName.segments.toSeq.size == 4)

    val byGenderAndName = {
      val op = CombineSegmentations()
      op(op.belongsTo1, byGender.belongsTo)(op.belongsTo2, byName.belongsTo).result
    }
    assert(byGenderAndName.segments.toSeq.size == 4)
    val segmentSizes = byGenderAndName.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 1, 1, 1))
  }

  test("example graph by gender and age") {
    val g = ExampleGraph()().result
    val byGender = {
      val op = StringBucketing()
      op(op.attr, g.gender).result
    }
    assert(byGender.segments.toSeq.size == 2)

    val byAge = {
      val op = DoubleBucketing(40, overlap = false)
      op(op.attr, g.age).result
    }
    assert(byAge.segments.toSeq.size == 2)

    val byGenderAndAge = {
      val op = CombineSegmentations()
      op(op.belongsTo1, byGender.belongsTo)(op.belongsTo2, byAge.belongsTo).result
    }
    assert(byGenderAndAge.segments.toSeq.size == 3)
    val segmentSizes = byGenderAndAge.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 1, 2))
  }
}
