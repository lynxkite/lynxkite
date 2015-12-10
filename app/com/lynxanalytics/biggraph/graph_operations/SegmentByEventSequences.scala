package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import org.apache.spark.Accumulator

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.Scripting._

trait TimeLineCrosser {
  // Takes a list of events and returns a list of cells. A cell is a list of events.
  def getSegments(events: Iterable[(Double, Long)]): Iterator[(Long, Seq[Long])]
}

// For each event E1, generates an E1, E2, ..., EN cell where N = timeWindowLength.
// The time difference between E1 and EN should be less than timeWindowLength.
case class ContinuousEventsCrosser(
    sequenceLength: Int,
    timeWindowStep: Double,
    timeWindowLength: Double) extends TimeLineCrosser {
  override def getSegments(events: Iterable[(Double, Long)]): Iterator[(Long, Seq[Long])] = {
    events.sliding(sequenceLength).flatMap {
      eventWindow =>
        {
          val (times, places) = eventWindow.unzip
          val firstTime = times.head
          val lastTime = times.last
          if (lastTime < firstTime + timeWindowLength) {
            val timeBucket = (firstTime / timeWindowStep).floor.round
            Some(timeBucket, places.toSeq)
          } else {
            None
          }
        }
    }
  }
}

// Starting from each event, takes the longest possible sequence starting there and no longer than timeWindowLength.
// From these, takes all possible event sublists of length sequenceLength.
object EventsWithGapsCrosser {
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
case class EventsWithGapsCrosser(
    sequenceLength: Int,
    timeWindowStep: Double,
    timeWindowLength: Double) extends TimeLineCrosser {
  override def getSegments(events: Iterable[(Double, Long)]): Iterator[(Long, Seq[Long])] = {
    val eventSeq = events.toSeq
    eventSeq.indices.flatMap {
      index =>
        {
          val subEventSeq = eventSeq.drop(index).takeWhile(_._1 < eventSeq(index)._1 + timeWindowLength)
          val (times, places) = subEventSeq.unzip
          val firstTime = times.head
          val timeBucket = (firstTime / timeWindowStep).floor.round

          EventsWithGapsCrosser
            .sublists(List(), places.toSeq, sequenceLength)
            .map { placesSubList => (timeBucket, placesSubList) }
        }
    }.distinct.iterator
  }
}

object SegmentByEventSequences extends OpFromJson {
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
    val segmentSize = vertexAttribute[Double](segments)
    val belongsTo = edgeBundle(
      inputs.personVs.entity,
      segments,
      EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) = SegmentByEventSequences(
    (j \ "algorithm").as[String],
    (j \ "sequenceLength").as[Int],
    (j \ "timeWindowStep").as[Double],
    (j \ "timeWindowLength").as[Double])
}
import SegmentByEventSequences._
case class SegmentByEventSequences(
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

  private def getAlgorithm(algorithmId: String): TimeLineCrosser = {
    algorithmId match {
      case "continuous" =>
        new ContinuousEventsCrosser(sequenceLength, timeWindowStep, timeWindowLength)
      case "with-gaps" =>
        new EventsWithGapsCrosser(sequenceLength, timeWindowStep, timeWindowLength)
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
      .sortUnique(partitioner)
    val personToEvents = eventToPerson
      .join(eventToLocationAndTime)
      .map { case (eventId, (personId, (locationId, time))) => personId -> (time, locationId) }
      .groupBySortedKey(partitioner)

    val personToSegmentCode = personToEvents.flatMapValues {
      events: Iterable[(Double, Long)] =>
        {
          val sortedEvents = events.toSeq.sortBy(_._1)
          getAlgorithm(algorithm).getSegments(sortedEvents)
        }
    }

    import scala.math.Ordering.Implicits._ // allow sorting by lists as keys
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
        case (_, persons) => persons.size.toDouble
      })
  }
}
