// Naming convention: "people" are attending "events", each "event" has a "location" and a "time".
// This operation puts people into segments based on sub-sequences of events of their event histories.
// See the implementations of the trait TimeLineSegmentGenerator for concrete algorithm implementations.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark.HashPartitioner

import scala.reflect.ClassTag

object SegmentByEventSequence extends OpFromJson {
  case class Event(time: Double, location: Long)
  case class EventSpan(startTime: Double, endTime: Double, location: Long)

  // The strategy used in the algorithm to assign a person into segments.
  trait TimeLineSegmentGenerator[SegmentIdType] {
    // Takes the complete list of events of a person and returns a list of segment
    // ids in which the person should belong.
    def getSegments(events: Iterable[Event]): Iterator[SegmentIdType]
  }

  // This identifies a segment by the bucket of the starting time and a list of locations.
  case class EventListSegmentId(timeBucket: Long, locations: Seq[Long]) {
  }
  implicit val eventListSegmentIdOrdering: Ordering[EventListSegmentId] = {
    import scala.math.Ordering.Implicits._
    Ordering.by(id => (id.timeBucket, id.locations))
  }

  object ContinuousEventsSegmentGenerator {
    def groupEventsByLocation(it: BufferedIterator[Event]): List[EventSpan] = {
      if (!it.hasNext) {
        List()
      } else {
        val first = it.next
        var last = first
        while (it.hasNext && it.head.location == first.location) {
          last = it.next
        }
        EventSpan(first.time, last.time, first.location) :: groupEventsByLocation(it)
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
      timeWindowLength: Double) extends TimeLineSegmentGenerator[EventListSegmentId] {

    override def getSegments(events: Iterable[Event]): Iterator[EventListSegmentId] = {
      val dedupedEvents = ContinuousEventsSegmentGenerator.groupEventsByLocation(events.iterator.buffered)
      dedupedEvents.sliding(sequenceLength).flatMap {
        eventWindow =>
          val firstEvent = eventWindow.head
          val lastEvent = eventWindow.last
          val locations = eventWindow.map { _.location }
          val earliestAllowedStart = lastEvent.endTime - timeWindowLength
          val minBucket = (Math.max(firstEvent.startTime, earliestAllowedStart) / timeWindowStep).floor.round
          val maxBucket = (firstEvent.endTime / timeWindowStep).floor.round
          // Can be empty if minBucket > maxBucket:
          (minBucket to maxBucket).map { bucket => EventListSegmentId(bucket, locations) }
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
  // Starting from each event, takes the longest possible sequence starting there and no longer in time than
  // timeWindowLength.
  // From these, takes all possible event sublists having number of events = sequenceLength.
  case class EventsWithGapsSegmentGenerator(
      sequenceLength: Int,
      timeWindowStep: Double,
      timeWindowLength: Double) extends TimeLineSegmentGenerator[EventListSegmentId] {
    override def getSegments(events: Iterable[Event]): Iterator[EventListSegmentId] = {
      val eventSeq = events.toSeq
      eventSeq.indices.flatMap {
        index =>
          {
            val subEventSeq = eventSeq.drop(index).takeWhile(_.time < eventSeq(index).time + timeWindowLength)
            val locations = subEventSeq.map { _.location }
            val firstTime = subEventSeq.head.time
            val timeBucket = (firstTime / timeWindowStep).floor.round

            EventsWithGapsSegmentGenerator
              .sublists(List(), locations.toSeq, sequenceLength)
              .map { locationsSubList => EventListSegmentId(timeBucket, locationsSubList) }
          }
      }.distinct.iterator
    }
  }

  class Input extends MagicInputSignature {
    val personVs = vertexSet
    val eventVs = vertexSet
    val locationVs = vertexSet
    val eventTimeAttribute = vertexAttribute[Double](eventVs)
    val personBelongsToEvent = edgeBundle(personVs, eventVs)
    val eventBelongsToLocation = edgeBundle(eventVs, locationVs)
  }
  class Output(
      implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val segments = vertexSet
    val segmentDescription = vertexAttribute[String](segments)
    val segmentSize = vertexAttribute[Double](segments)
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
  extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "algorithm" -> algorithm,
    "sequenceLength" -> sequenceLength,
    "timeWindowStep" -> timeWindowStep,
    "timeWindowLength" -> timeWindowLength)

  def executeWithAlgorithm[T](
    inputDatas: DataSet,
    o: Output,
    output: OutputBuilder,
    rc: RuntimeContext,
    segmentGenerator: TimeLineSegmentGenerator[T])(
    implicit
    ct: ClassTag[T], ordering: Ordering[T]): Unit = {

    implicit val id = inputDatas
    val eventTimeAttributeRdd = inputs.eventTimeAttribute.rdd
    val personBelongsToEventRdd = inputs.personBelongsToEvent.rdd
    val eventBelongsToLocationRdd = inputs.eventBelongsToLocation.rdd
    val partitioner = new HashPartitioner(
      eventTimeAttributeRdd.partitions.length +
        personBelongsToEventRdd.partitions.length +
        eventBelongsToLocationRdd.partitions.length)
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
          val sortedEvents = events.toSeq
            .map { case (time, location) => Event(time, location) }
            .sortBy(_.time)
          segmentGenerator.getSegments(sortedEvents)
        }
    }
    val segmentIdToCodeAndPersons = personToSegmentCode
      .map(_.swap)
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
        case (_, persons) => persons.size.toDouble
      })
  }

  def execute(
    inputDatas: DataSet,
    o: Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    algorithm match {
      case "continuous" =>
        executeWithAlgorithm(
          inputDatas,
          o,
          output,
          rc,
          new ContinuousEventsSegmentGenerator(sequenceLength, timeWindowStep, timeWindowLength))
      case "with-gaps" =>
        executeWithAlgorithm(
          inputDatas,
          o,
          output,
          rc,
          new EventsWithGapsSegmentGenerator(sequenceLength, timeWindowStep, timeWindowLength))
    }
  }
}
