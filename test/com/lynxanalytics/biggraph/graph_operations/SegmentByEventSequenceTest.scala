package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class SegmentByEventSequenceTest extends FunSuite with TestGraphOp {
  test("ContinuousEventCrosser.groupEventsByLocation") {
    val events = Seq((1.0, 1l), (1.1, 2l), (1.2, 2l), (1.3, 3l), (1.4, 3l), (1.5, 3l), (1.6, 1l), (1.7, 1l))
    val grouped = ContinuousEventsSegmentGenerator.groupEventsByLocation(events)
    assert(grouped.toSeq == Seq((1.0, 1.0, 1l), (1.1, 1.2, 2l), (1.3, 1.5, 3l), (1.6, 1.7, 1l)))
  }

  test("EventsWithGapsCroesser.sublists") {
    val events = Seq((1.0, 1l), (1.1, 2l), (1.2, 3l), (1.3, 3l))
    val sublists = EventsWithGapsSegmentGenerator.sublists(List(), events, 2)
    import scala.math.Ordering.Implicits._
    assert(sublists.toSeq.sorted == Seq(
      List((1.0, 1l), (1.1, 2l)),
      List((1.0, 1l), (1.2, 3l)),
      List((1.0, 1l), (1.3, 3l)),
      List((1.1, 2l), (1.2, 3l)),
      List((1.1, 2l), (1.3, 3l)),
      List((1.2, 3l), (1.3, 3l))
    ).sorted)
  }

  test("ConinuousEventsSegmentGenerator") {
    val events = Seq((1.0, 1l), (1.1, 2l), (1.21, 2l), (1.3, 3l), (1.4, 3l), (1.5, 3l), (1.6, 1l), (1.7, 1l))
    val segments = ContinuousEventsSegmentGenerator(3, 0.1, 5.0).getSegments(events)
    assert(
      segments.toSeq ==
        Seq(
          EventListSegmentId(10, Seq(1l, 2l, 3l)),
          EventListSegmentId(11, Seq(2l, 3l, 1l)),
          EventListSegmentId(12, Seq(2l, 3l, 1l))))
  }

  test("EventsWithGapsSegmentGenerator") {
    val events = Seq((1.01, 1l), (1.11, 2l), (1.21, 3l), (1.31, 3l))
    val segments: Iterator[TimeLineCrosserSegmentId] = EventsWithGapsSegmentGenerator(2, 0.1, 0.31).getSegments(events)
    val golden: Seq[TimeLineCrosserSegmentId] =
      Seq(
        EventListSegmentId(10, Seq(1l, 2l)),
        EventListSegmentId(10, Seq(1l, 3l)),
        EventListSegmentId(10, Seq(2l, 3l)),
        EventListSegmentId(10, Seq(3l, 3l)),
        EventListSegmentId(11, Seq(2l, 3l)),
        EventListSegmentId(11, Seq(3l, 3l)),
        EventListSegmentId(12, Seq(3l, 3l)))
    assert(segments.toSeq.sorted == golden.sorted)
  }

  test("Run SegmentByEventSequence operation") {
    // Build test graph:
    val personVs = SmallTestGraph((0 to 2).map(id => id -> Seq()).toMap).result.vs
    val eventVs = SmallTestGraph((0 to 8).map(id => id -> Seq()).toMap).result.vs
    val locationVs = SmallTestGraph((0 to 3).map(id => id -> Seq()).toMap).result.vs
    val personBelongsToEvent = {
      val op = AddEdgeBundle(Seq(
        (0, 0), (0, 1), (0, 2),
        (1, 3), (1, 4), (1, 5),
        (2, 6), (2, 7), (2, 8)))
      op(op.vsA, personVs)(op.vsB, eventVs).result.esAB
    }
    val eventTimes = {
      val op = AddDoubleVertexAttribute(Map(
        0 -> 0, 1 -> 1, 2 -> 2,
        3 -> 0, 4 -> 1, 5 -> 2,
        6 -> 0, 7 -> 1, 8 -> 2))
      op(op.vs, eventVs).result.attr
    }
    val eventBelongsToLocation = {
      val op = AddEdgeBundle(Seq(
        (0, 0), (1, 0), (2, 1),
        (3, 2), (4, 0), (5, 1),
        (6, 2), (7, 2), (8, 2)))
      op(op.vsA, eventVs)(op.vsB, locationVs).result.esAB
    }

    // Run operation:
    val result = {
      val op = SegmentByEventSequence("continuous", 2, 0.9, 2.1)
      op(op.personVs, personVs)(
        op.eventVs, eventVs)(
          op.locationVs, locationVs)(
            op.eventTimeAttribute, eventTimes)(
              op.personBelongsToEvent, personBelongsToEvent)(
                op.eventBelongsToLocation, eventBelongsToLocation).result
    }
    // Ensure that person vertices #0 and #1 are sharing a segment, but no other pairs
    // are sharing a vertex.
    val segmentList = result.belongsTo.rdd
      .map { case (_, Edge(member, segment)) => (segment, member) }
      .groupByKey
      .map { case (_, members) => members }
      .collect
      .toSeq
      .sortBy { members => -members.size } // sort by decreasing segment size
    assert(segmentList(0) == Seq(0, 1))
    assert(segmentList(1).size == 1) // subsequent segments are size of 1
  }
}
