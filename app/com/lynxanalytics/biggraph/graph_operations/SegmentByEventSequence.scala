// Naming convention: "people" are attending "events", each "event" has a "location" and a "time".
// This operation puts people into segments based on sub-sequences of events of their event histories.
// See the implementations of the trait TimeLineCrosser for concrete algorithm implementations.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Each segment in which "persons" will be assigned to, will be defined by such
// an identifier:
trait TimeLineCrosserSegmentId extends Ordered[TimeLineCrosserSegmentId] {
}

// The strategy used in the algorithm to assign a "person" into segments.
trait TimeLineSegmentGenerator {
  // Takes a list of events of a "person" and returns a list of segment ids in which the
  // person should belong.
  def getSegments(events: Iterable[(Double, Long)]): Iterator[TimeLineCrosserSegmentId]
}

// This identifies a segment by the bucket of the starting time and a list of locations.
case class EventListSegmentId(id: (Long, Seq[Long])) extends TimeLineCrosserSegmentId {
  override def compare(otherObj: TimeLineCrosserSegmentId): Int = {
    val other = otherObj.asInstanceOf[EventListSegmentId]
    import scala.math.Ordering.Implicits._
    import scala.math.Ordered.orderingToOrdered
    id.compare(other.id)
  }
}

object ContinuousEventsSegmentGenerator {
  def groupEventsByLocation(seq: Iterable[(Double, Long)]): Iterator[(Double, Double, Long)] = {
    Iterator.iterate((seq, Iterable[(Double, Long)]())) {
      case (Seq(), _) =>
        (Seq(), Seq())
      case (seq, _) => {
        val head = seq.head
        (seq.dropWhile { x => x._2 == head._2 }, seq.takeWhile { x => x._2 == head._2 })
      }
    }.takeWhile {
      case (input, part) => input.nonEmpty || part.nonEmpty
    }.flatMap {
      case (_, Seq()) =>
        None
      case (_, part) => {
        Some(part.head._1, part.last._1, part.head._2)
      }
    }
  }
}
// Generates continuous event lists as segments. In the input event list, subsequent
// events at the same location are merged, and then all continuous sequences with max
// |sequenceLength| items and |timeWindowLength| length are enumerated. Then a segment
// is emitted for each time bucket falling into the time range of the first (possible merged)
// event.
case class ContinuousEventsSegmentGenerator(
    sequenceLength: Int,
    timeWindowStep: Double,
    timeWindowLength: Double) extends TimeLineSegmentGenerator {

  override def getSegments(events: Iterable[(Double, Long)]): Iterator[EventListSegmentId] = {
    val dedupedEvents = ContinuousEventsSegmentGenerator.groupEventsByLocation(events)
    dedupedEvents.sliding(sequenceLength).flatMap {
      eventWindow =>
        {
          val (startTimes, endTimes, places) = eventWindow.unzip3
          val endTime = endTimes.last
          val minStartTime = startTimes.head
          val maxStartTime = endTimes.head
          val earliestAllowedStart = endTime - timeWindowLength
          if (earliestAllowedStart <= maxStartTime) {
            val minBucket = (Math.max(minStartTime, earliestAllowedStart) / timeWindowStep).floor.round
            val maxBucket = (maxStartTime / timeWindowStep).floor.round
            (minBucket to maxBucket).map { bucket => EventListSegmentId((bucket, places)) }
          } else {
            Seq()
          }
        }
    }
  }
}

object EventsWithGapsSegmentGenerator {
  def sublists[T](seed: List[T], list: Seq[T], numToTake: Int): Set[List[T]] = {
    if (numToTake > list.length) {
      Set()
    } else if (numToTake == 0) {
      Set(seed)
    } else {
      sublists(seed :+ list.head, list.tail, numToTake - 1) ++
        sublists(seed, list.tail, numToTake)
    }
  }
}
// Starting from each event, takes the longest possible sequence starting there and no longer than timeWindowLength.
// From these, takes all possible event sublists of length sequenceLength.
case class EventsWithGapsSegmentGenerator(
    sequenceLength: Int,
    timeWindowStep: Double,
    timeWindowLength: Double) extends TimeLineSegmentGenerator {
  override def getSegments(events: Iterable[(Double, Long)]): Iterator[EventListSegmentId] = {
    val eventSeq = events.toSeq
    eventSeq.indices.flatMap {
      index =>
        {
          val subEventSeq = eventSeq.drop(index).takeWhile(_._1 < eventSeq(index)._1 + timeWindowLength)
          val (times, places) = subEventSeq.unzip
          val firstTime = times.head
          val timeBucket = (firstTime / timeWindowStep).floor.round

          EventsWithGapsSegmentGenerator
            .sublists(List(), places.toSeq, sequenceLength)
            .map { placesSubList => EventListSegmentId((timeBucket, placesSubList)) }
        }
    }.distinct.iterator
  }
}

object SegmentByEventSequence extends OpFromJson {
  class Input extends MagicInputSignature {
    val personVs = vertexSet
    val eventVs = vertexSet
    val locationVs = vertexSet
    val eventTimeAttribute = vertexAttribute[Double](eventVs)
    val personBelongsToEvent = edgeBundle(personVs, eventVs)
    val eventBelongsToLocation = edgeBundle(eventVs, locationVs)
  }
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val segments = vertexSet
    val segmentDescription = vertexAttribute[String](segments)
    val segmentSize = vertexAttribute[Long](segments)
    val belongsTo = edgeBundle(
      inputs.personVs.entity,
      segments,
      EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) = SegmentByEventSequence(
    (j \ "algorithm").as[String],
    (j \ "sequenceLength").as[Int],
    (j \ "timeWindowStep").as[Double],
    (j \ "timeWindowLength").as[Double])
}
import SegmentByEventSequence._
case class SegmentByEventSequence(
  algorithm: String,
  sequenceLength: Int,
  timeWindowStep: Double,
  timeWindowLength: Double)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "algorithm" -> algorithm,
    "sequenceLength" -> sequenceLength,
    "timeWindowStep" -> timeWindowStep,
    "timeWindowLength" -> timeWindowLength)

  private def getAlgorithm(algorithmId: String): TimeLineSegmentGenerator = {
    algorithmId match {
      case "continuous" =>
        new ContinuousEventsSegmentGenerator(sequenceLength, timeWindowStep, timeWindowLength)
      case "with-gaps" =>
        new EventsWithGapsSegmentGenerator(sequenceLength, timeWindowStep, timeWindowLength)
    }
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val eventTimeAttributeRdd = inputs.eventTimeAttribute.rdd
    val personBelongsToEventRdd = inputs.personBelongsToEvent.rdd
    val eventBelongsToLocationRdd = inputs.eventBelongsToLocation.rdd
    val partitioner = eventBelongsToLocationRdd.partitioner.get
    val eventToLocationAndTime = eventBelongsToLocationRdd
      .map { case (edgeId, Edge(eventId, locationId)) => (eventId, locationId) }
      .sort(partitioner)
      .sortedJoin(eventTimeAttributeRdd.sortedRepartition(partitioner)) // eventid -> (locationId, time)
    val eventToPerson = personBelongsToEventRdd
      .map { case (edgeId, Edge(personId, eventId)) => (eventId, personId) }
      .sort(partitioner)
    val personToEvents = eventToPerson
      .sortedJoinWithDuplicates(eventToLocationAndTime)
      .map { case (eventId, (personId, (locationId, time))) => personId -> (time, locationId) }
      .groupBySortedKey(partitioner)

    val personToSegmentCode = personToEvents.flatMapValues {
      events: Iterable[(Double, Long)] =>
        {
          val sortedEvents = events.toSeq.sortBy(_._1)
          getAlgorithm(algorithm).getSegments(sortedEvents)
        }
    }

    val segmentIdToCodeAndPersons = personToSegmentCode
      .map { case (personId, segmentCode) => (segmentCode, personId) }
      .groupBySortedKey(partitioner)
      .randomNumbered(partitioner)
    output(
      o.belongsTo,
      segmentIdToCodeAndPersons
        .flatMapValues { case (code, persons) => persons } // (segmentId -> personId)
        .map { case (segmentId, personId) => Edge(personId, segmentId) }
        .randomNumbered(partitioner))
    output(
      o.segments,
      segmentIdToCodeAndPersons.mapValues { _ => () })
    output(
      o.segmentDescription,
      segmentIdToCodeAndPersons.mapValues {
        case (segmentCode, _) => segmentCode.toString
      })
    output(
      o.segmentSize,
      segmentIdToCodeAndPersons.mapValues {
        case (_, persons) => persons.size.toLong
      })
  }
}
