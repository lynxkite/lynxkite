// Adds scale-free edges to a graph based on the popularity x similarity model.
// The added eges will be scale-free and have high average clustering.
// Based on paper: https://arxiv.org/abs/1205.4384
package com.lynxanalytics.biggraph.graph_operations

import scala.math
import scala.util.Random
import scala.collection.immutable.SortedMap
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class HyperVertex(
    id: Long,
    ord: Long,
    radial: Double,
    angular: Double,
    expectedDegree: Double)

class LinkedHyperVertex(val vertex: HyperVertex) {
  var previous: LinkedHyperVertex = this
  var next: LinkedHyperVertex = this
  var radialPrevious: LinkedHyperVertex = this
}

object PSOGenerator extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(
      implicit
      instance: MetaGraphOperationInstance,
      inputs: Input)
      extends MagicOutput(instance) {
    val es = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val radial = vertexAttribute[Double](inputs.vs.entity)
    val angular = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = PSOGenerator(
    (j \ "externaldegree").as[Double],
    (j \ "internaldegree").as[Double],
    (j \ "exponent").as[Double],
    (j \ "seed").as[Long])
}
import PSOGenerator._
case class PSOGenerator(externalDegree: Double, internalDegree: Double, exponent: Double, seed: Long)
    extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "externaldegree" -> externalDegree,
    "internaldegree" -> internalDegree,
    "exponent" -> exponent,
    "seed" -> seed)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = inputs.vs.rdd.partitioner.get
    val size = inputs.vs.data.count.getOrElse(inputs.vs.rdd.count)
    val logSize = math.log(size)
    // +logSize instead of +1 to account for popularity fading
    val ordinals = inputs.vs.rdd.keys.zipWithIndex.map { case (id, ordinal) => (id, ordinal + logSize.toInt) }
    val sc = rc.sparkContext
    // Adds the necessary attributes for later calculations.
    // ord needs to be 1-indexed as log(0) will break things.
    val vertices = ordinals.mapPartitionsWithIndex {
      case (pid, iter) =>
        val rnd = new Random((pid << 16) + seed)
        iter.map {
          case (id, ordinal) =>
            HyperVertex(
              id = id,
              ord = ordinal,
              radial = 2 * math.log(ordinal),
              angular = rnd.nextDouble * math.Pi * 2,
              expectedDegree = HyperDistance.totalExpectedEPSO(
                exponent,
                externalDegree,
                internalDegree,
                size,
                ordinal),
            )
        }
    }
    // For each vertex: samples ~log(n) vertices with smallest angular coordinate difference, plus
    // preceding appearance (abstraction for higher popularity vertex; smaller radial coordinate).
    // Groups the samples for each vertex into a list.
    val allVerticesList: List[HyperVertex] = vertices.collect().sortBy(_.angular).toList
    // Constructs list double linked by angular, single linked by ord/radial
    val lastLHV = new LinkedHyperVertex(allVerticesList.head)
    var linkedList = lastLHV :: Nil
    for (vertex <- allVerticesList.tail) {
      val newLHV = new LinkedHyperVertex(vertex)
      newLHV.previous = linkedList.head
      linkedList.head.next = newLHV
      linkedList = newLHV :: linkedList
    }
    lastLHV.previous = linkedList.head
    linkedList.head.next = lastLHV

    linkedList.sortBy(_.vertex.ord)
    var radPrevTracker = linkedList.head
    for (lhv <- linkedList.tail) {
      lhv.radialPrevious = radPrevTracker
      radPrevTracker = lhv
    }
    val possibilityList: List[List[HyperVertex]] = linkedList.map {
      lhv =>
        HyperDistance.linkedListSampler(
          (logSize * lhv.vertex.expectedDegree).toInt,
          lhv,
          lhv.previous,
          lhv.next,
          lhv.radialPrevious,
          Nil)
    }
    // Selects the expectedDegree smallest distance edges from possibility bundles.
    val possibilities = sc.parallelize(possibilityList)
    val es = possibilities.flatMap {
      data =>
        val numSelections: Int = data.head.expectedDegree.toInt
        val src = data.head
        val dst = data.tail.map {
          dst => (HyperDistance.hyperbolicDistance(src, dst), Edge(src.id, dst.id))
        }.sortBy(_._1)
        dst.take(numSelections).map { case (key, value) => value }
    }.flatMap { edge => List(edge, Edge(edge.dst, edge.src)) }
      .distinct

    output(o.radial, vertices.map { v => (v.id, v.radial) }.sortUnique(partitioner))
    output(o.angular, vertices.map { v => (v.id, v.angular) }.sortUnique(partitioner))
    output(o.es, es.randomNumbered(partitioner))
  }
}
object HyperDistance {
  // Returns hyperbolic distance.
  def hyperbolicDistance(src: HyperVertex, dst: HyperVertex): Double = {
    src.radial + dst.radial + 2 * math.log(phi(src.angular, dst.angular) / 2)
  }
  // Returns angular component for hyperbolic distance calculation.
  def phi(ang1: Double, ang2: Double): Double = {
    math.Pi - math.abs(math.Pi - math.abs(ang1 - ang2))
  }
  // Expected number of internal connections at given time in the E-PSO model.
  def internalConnectionsEPSO(
      exponent: Double,
      internalLinks: Double,
      maxNodes: Long,
      ord: Long): Double = {
    val firstPart: Double = ((2 * internalLinks * (1 - exponent)) /
      (math.pow(1 - math.pow(maxNodes.toDouble, -(1 - exponent)), 2) * (2 * exponent - 1)))
    val secondPart: Double = math.pow((maxNodes / ord.toDouble), 2 * exponent - 1) - 1
    val thirdPart: Double = (1 - math.pow(ord.toDouble, -(1 - exponent)))
    firstPart * secondPart * thirdPart
  }
  // Expected number of connections at given time in the E-PSO model.
  def totalExpectedEPSO(
      exponent: Double,
      externalLinks: Double,
      internalLinks: Double,
      maxNodes: Long,
      ord: Long): Double = {
    externalLinks + internalConnectionsEPSO(exponent, internalLinks, maxNodes, ord)
  }
  // Equation for parameter denoted I_i in the HyperMap paper.
  def inverseExponent(ord: Long, exponent: Double): Double = {
    (1 / (1 - exponent)) * (1 - math.pow(ord, -(1 - exponent)))
  }
  // Expected number of connections for a vertex, used in calculating angular.
  def expectedConnections(
      vertex: HyperVertex,
      exponent: Double,
      temperature: Double,
      externalLinks: Double): Double = {
    val firstPart: Double = (2 * temperature) / math.sin(temperature * math.Pi)
    val secondPart: Double = inverseExponent(vertex.ord, exponent) / externalLinks
    val logged: Double = math.log(firstPart * secondPart)
    vertex.radial - (2 * logged)
  }
  // Connection probability.
  def probability(
      first: HyperVertex,
      second: HyperVertex,
      exponent: Double,
      temperature: Double,
      externalLinks: Double): Double = {
    val dist: Double = hyperbolicDistance(first, second)
    1 / (1 + math.exp((1 / (2 * temperature)) * (dist -
      expectedConnections(first, exponent, temperature, externalLinks))))
  }
  @annotation.tailrec
  final def linkedListSampler(
      remainingIterations: Int,
      starter: LinkedHyperVertex,
      previous: LinkedHyperVertex,
      next: LinkedHyperVertex,
      radialPrevious: LinkedHyperVertex,
      samplesSoFar: List[HyperVertex]): List[HyperVertex] = {
    val newSamplesSoFar = {
      if (radialPrevious != starter) {
        radialPrevious.vertex :: next.vertex :: previous.vertex :: samplesSoFar
      } else next.vertex :: previous.vertex :: samplesSoFar
    }
    val newPrevious = previous.previous
    val newNext = next.next
    val newRadialPrevious = radialPrevious.radialPrevious
    if (remainingIterations < 1) starter.vertex :: samplesSoFar
    else linkedListSampler(remainingIterations - 1, starter, newPrevious, newNext, newRadialPrevious, newSamplesSoFar)
  }
}
