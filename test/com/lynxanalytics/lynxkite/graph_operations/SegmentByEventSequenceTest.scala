package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

import SegmentByEventSequence._

class SegmentByEventSequenceTest extends AnyFunSuite with TestGraphOp {
  test("ContinuousEventCrosser.groupEventsByLocation") {
    val events = Seq(
      Event(1.0, 1L),
      Event(1.1, 2L),
      Event(1.2, 2L),
      Event(1.3, 3L),
      Event(1.4, 3L),
      Event(1.5, 3L),
      Event(1.6, 1L),
      Event(1.7, 1L))
    val grouped = ContinuousEventsSegmentGenerator.groupEventsByLocation(events.iterator.buffered)
    assert(grouped.toSeq ==
      Seq(
        EventSpan(1.0, 1.0, 1L),
        EventSpan(1.1, 1.2, 2L),
        EventSpan(1.3, 1.5, 3L),
        EventSpan(1.6, 1.7, 1L)))
  }

  test("EventsWithGapsCroesser.sublists") {
    val events = Seq((1.0, 1L), (1.1, 2L), (1.2, 3L), (1.3, 3L))
    val sublists = EventsWithGapsSegmentGenerator.sublists(List(), events, 2)
    import scala.math.Ordering.Implicits._
    assert(sublists.toSeq.sorted == Seq(
      List((1.0, 1L), (1.1, 2L)),
      List((1.0, 1L), (1.2, 3L)),
      List((1.0, 1L), (1.3, 3L)),
      List((1.1, 2L), (1.2, 3L)),
      List((1.1, 2L), (1.3, 3L)),
      List((1.2, 3L), (1.3, 3L))).sorted)
  }

  test("ConinuousEventsSegmentGenerator") {
    val events = Seq(
      Event(1.0, 1L),
      Event(1.1, 2L),
      Event(1.21, 2L),
      Event(1.3, 3L),
      Event(1.4, 3L),
      Event(1.5, 3L),
      Event(1.6, 1L),
      Event(1.7, 1L))
    val segments = ContinuousEventsSegmentGenerator(3, 0.1, 5.0).getSegments(events)
    assert(
      segments.toSeq ==
        Seq(
          EventListSegmentId(10, Seq(1L, 2L, 3L)),
          EventListSegmentId(11, Seq(2L, 3L, 1L)),
          EventListSegmentId(12, Seq(2L, 3L, 1L))))
  }

  test("EventsWithGapsSegmentGenerator") {
    val events = Seq(Event(1.01, 1L), Event(1.11, 2L), Event(1.21, 3L), Event(1.31, 3L))
    val segments = EventsWithGapsSegmentGenerator(2, 0.1, 0.31).getSegments(events)
    val golden =
      Seq(
        EventListSegmentId(10, Seq(1L, 2L)),
        EventListSegmentId(10, Seq(1L, 3L)),
        EventListSegmentId(10, Seq(2L, 3L)),
        EventListSegmentId(10, Seq(3L, 3L)),
        EventListSegmentId(11, Seq(2L, 3L)),
        EventListSegmentId(11, Seq(3L, 3L)),
        EventListSegmentId(12, Seq(3L, 3L)),
      )
    assert(segments.toSeq.sorted == golden.sorted)
  }

  test("Run SegmentByEventSequence operation") {
    // Build test graph:
    val personVs = SmallTestGraph((0 to 2).map(id => id -> Seq()).toMap).result.vs
    val eventVs = SmallTestGraph((0 to 8).map(id => id -> Seq()).toMap).result.vs
    val locationVs = SmallTestGraph((0 to 3).map(id => id -> Seq()).toMap).result.vs
    val personBelongsToEvent = {
      val op = AddEdgeBundle(Seq(
        (0, 0),
        (0, 1),
        (0, 2),
        (1, 3),
        (1, 4),
        (1, 5),
        (2, 6),
        (2, 7),
        (2, 8)))
      op(op.vsA, personVs)(op.vsB, eventVs).result.esAB
    }
    val eventTimes = AddVertexAttribute.run(
      eventVs,
      Map(
        0 -> 0.0,
        1 -> 1.0,
        2 -> 2.0,
        3 -> 0.0,
        4 -> 1.0,
        5 -> 2.0,
        6 -> 0.0,
        7 -> 1.0,
        8 -> 2.0))
    val eventBelongsToLocation = {
      val op = AddEdgeBundle(Seq(
        (0, 0),
        (1, 0),
        (2, 1),
        (3, 2),
        (4, 0),
        (5, 1),
        (6, 2),
        (7, 2),
        (8, 2)))
      op(op.vsA, eventVs)(op.vsB, locationVs).result.esAB
    }

    // Run operation:
    val result = {
      val op = SegmentByEventSequence("continuous", 2, 0.9, 2.1)
      op(op.personVs, personVs)(
        op.eventVs,
        eventVs)(
        op.locationVs,
        locationVs)(
        op.eventTimeAttribute,
        eventTimes)(
        op.personBelongsToEvent,
        personBelongsToEvent)(
        op.eventBelongsToLocation,
        eventBelongsToLocation).result
    }
    // Ensure that person vertices #0 and #1 are sharing a segment, but no other pairs
    // are sharing a vertex.
    val segmentList = result.belongsTo.rdd
      .map { case (_, Edge(member, segment)) => (segment, member) }
      .collect
      .toSeq
      .groupBy { case (segment, _) => segment }
      .map { case (_, members) => members.map { case (segment, member) => member } }
      .toSeq // each item in our list is a segment now, represented by a list of its members
      .sortBy { members => -members.size } // sort by decreasing segment size
    assert(segmentList(0) == Seq(0, 1)) // the first (largest) segment has vertices 0 and 1 as members
    assert(segmentList(1).size == 1) // the subsequent (smaller) segments are size of 1
  }
}
