// Generates scale-free graph based on probability x similarity model.
// The degree distribution of the resulting graph will be scale-free and
// it will have high average clustering.
// Based on paper: https://www.caida.org/publications/papers/2015/network_mapping_replaying_hyperbolic/network_mapping_replaying_hyperbolic.pdf
package com.lynxanalytics.biggraph.graph_operations

import scala.math
import scala.util.Random
import scala.collection.immutable.SortedMap
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class HyperVertex(id: Long,
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

  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val (vs, es) = graph
    val radial = vertexAttribute[Double](vs)
    val angular = vertexAttribute[Double](vs)
  }
  def fromJson(j: JsValue) = PSOGenerator(
    (j \ "size").as[Long],
    (j \ "externaldegree").as[Double],
    (j \ "internaldegree").as[Double],
    (j \ "exponent").as[Double],
    (j \ "seed").as[Long])
}
import PSOGenerator._
case class PSOGenerator(size: Long, externalDegree: Double, internalDegree: Double,
                        exponent: Double, seed: Long) extends TypedMetaGraphOp[NoInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new NoInput

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj(
    "size" -> size,
    "externaldegree" -> externalDegree,
    "internaldegree" -> internalDegree,
    "exponent" -> exponent,
    "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val partitioner = rc.partitionerForNRows(size)
    val ordinals = rc.sparkContext.parallelize(0L until size,
      partitioner.numPartitions).randomNumbered(partitioner)
    val sc = rc.sparkContext
    val logSize = math.log(size)
    // Adds the necessary attributes for later calculations.
    // ord needs to be 1-indexed as log(0) will break things.
    val vertices = ordinals.mapPartitionsWithIndex {
      case (pid, iter) =>
        val rnd = new Random((pid << 16) + seed)
        iter.map {
          case (id, ordinal) =>
            HyperVertex(
              id = id,
              ord = ordinal + 1,
              radial = math.log(ordinal + 1),
              angular = rnd.nextDouble * math.Pi * 2,
              expectedDegree = totalExpectedEPSO(exponent,
                externalDegree, internalDegree, size, ordinal + 1))
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
      (lhv) =>
        var sampleList: List[HyperVertex] = Nil
        val numSamples = (logSize * lhv.vertex.expectedDegree).toInt
        var ne = lhv.next
        var pr = lhv.previous
        var rapr = lhv.radialPrevious
        for (i <- 0 until numSamples) {
          sampleList = ne.vertex :: pr.vertex :: sampleList
          if (rapr != lhv) sampleList = rapr.vertex :: sampleList
          ne = ne.next
          pr = pr.previous
          rapr = rapr.radialPrevious
        }
        lhv.vertex :: sampleList
    }
    // Selects the expectedDegree smallest distance edges from possibility bundles.
    val possibilities = sc.parallelize(possibilityList)
    val es = possibilities.flatMap {
      (data) =>
        val numSelections: Int = data.head.expectedDegree.toInt
        val src = data.head
        val dst = data.tail.map {
          (dst) => (hyperbolicDistance(src, dst), Edge(src.id, dst.id))
        }.sorted
        dst.take(numSelections + 1).map { case (key, value) => value }
    }.flatMap { (edge) => List(edge, Edge(edge.dst, edge.src)) }
      .distinct

    output(o.vs, ordinals.mapValues(_ => ()))
    output(o.radial, vertices.map { v => (v.id, v.radial) }.sortUnique(partitioner))
    output(o.angular, vertices.map { v => (v.id, v.angular) }.sortUnique(partitioner))
    output(o.es, es.randomNumbered(partitioner))
  }
  // Returns hyperbolic distance.
  def hyperbolicDistance(src: HyperVertex, dst: HyperVertex): Double = {
    src.radial + src.radial + 2 * math.log(phi(src.angular, dst.angular) / 2)
  }
  // Returns angular component for hyperbolic distance calculation.
  def phi(ang1: Double, ang2: Double): Double = {
    math.Pi - math.abs(math.Pi - math.abs(ang1 - ang2))
  }
  // Expected number of internal connections at given time in the E-PSO model.
  def internalConnectionsEPSO(exponent: Double,
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
  def totalExpectedEPSO(exponent: Double,
                        externalLinks: Double,
                        internalLinks: Double,
                        maxNodes: Long,
                        ord: Long): Double = {
    externalLinks + internalConnectionsEPSO(exponent, internalLinks, maxNodes, ord)
  }
}
