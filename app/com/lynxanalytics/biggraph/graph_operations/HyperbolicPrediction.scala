// Predicts edges in a graph that has hyperbolic coordinate attributes.
// Runs PSOGenerator with the already existing coordinates, but higher degrees.
// Takes the top X most likely edges and applies distinct.
package com.lynxanalytics.biggraph.graph_operations

import scala.math
import scala.util.Random
import scala.collection.immutable.SortedMap
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object HyperbolicPrediction extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val radial = vertexAttribute[Double](vs)
    val angular = vertexAttribute[Double](vs)
  }
  class Output(
      implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val predictedEdges = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = HyperbolicPrediction(
    (j \ "size").as[Int],
    (j \ "exponent").as[Double])
}
import HyperbolicPrediction._
case class HyperbolicPrediction(size: Int,
                                exponent: Double) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "size" -> size,
    "exponent" -> exponent)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = inputs.vs.rdd.partitioner.get
    val vertexSetSize = inputs.vs.data.count.getOrElse(inputs.vs.rdd.count)
    val edgeBundleSize = inputs.es.data.count.getOrElse(inputs.es.rdd.count)
    val sc = rc.sparkContext
    val logVertexSetSize = math.log(vertexSetSize)
    val ordinals = inputs.radial.rdd.sortedLeftOuterJoin(inputs.angular.rdd)
      .map { case (id, (rad, ang)) => (id, rad, ang.getOrElse(0.0)) }
      .sortBy(_._2)
      .zipWithIndex
      .map { case ((id, rad, ang), ord) => (id, rad, ang, ord) }
    val internalDegree = (edgeBundleSize / vertexSetSize.toDouble / 2) *
      ((edgeBundleSize + size) / edgeBundleSize.toDouble)
    val externalDegree = internalDegree
    val vertices = ordinals.map {
      case (id, radi, angu, ordi) =>
        HyperVertex(
          id = id,
          ord = ordi,
          radial = radi,
          angular = angu,
          expectedDegree = totalExpectedEPSO(exponent,
            externalDegree, internalDegree, size, ordi + 1))
    }
    // For each vertex: samples ~log(n) vertices with smallest angular coordinate difference, plus
    // preceding appearance (abstraction for higher popularity vertex; smaller radial coordinate).
    // Groups the samples for each vertex into a list.
    val allVerticesList: List[HyperVertex] = vertices.collect().sortBy(_.angular).toList
    // Constructs list double linked by angular, single linked by ord/radial.
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
        var sampleList: List[HyperVertex] = Nil
        val numSamples = (logVertexSetSize * lhv.vertex.expectedDegree).toInt
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
    // Takes the $size most probable edges from all generated edges and adds them.
    val possibilities = sc.parallelize(possibilityList)
    val extraEdges = possibilities.flatMap {
      data =>
        val numSelections: Int = data.head.expectedDegree.toInt
        val src = data.head
        val dst = data.tail.map {
          dst =>
            (probability(src, dst, exponent, 0.45, externalDegree),
              Edge(src.id, dst.id))
        }.sortBy(-_._1)
        dst.take(numSelections)
    }.top(size)
      .map { case (prob, edge) => edge }
    val edgesWithoutIDs = inputs.es.rdd.map { case (id, edge) => edge }
    val edgesPlusNew = edgesWithoutIDs ++ sc.parallelize(extraEdges)
    val predictedEdges = edgesPlusNew.flatMap {
      case (edge) =>
        List(edge, Edge(edge.dst, edge.src))
    }.distinct
    output(o.predictedEdges, predictedEdges.randomNumbered(partitioner))

  }
  // Returns hyperbolic distance.
  def hyperbolicDistance(first: HyperVertex, second: HyperVertex): Double = {
    first.radial + second.radial + 2 * math.log(phi(first.angular, second.angular) / 2)
  }
  // Returns angular component for hyperbolic distance.
  def phi(ang1: Double, ang2: Double): Double = {
    math.Pi - math.abs(math.Pi - math.abs(ang1 - ang2))
  }
  // Equation for parameter denoted I_i in the HyperMap paper.
  def inverseExponent(ord: Long, exponent: Double): Double = {
    (1 / (1 - exponent)) * (1 - math.pow(ord, -(1 - exponent)))
  }
  // Expected number of connections for a vertex, used in calculating angular.
  def expectedConnections(vertex: HyperVertex,
                          exponent: Double,
                          temperature: Double,
                          externalLinks: Double): Double = {
    val firstPart: Double = (2 * temperature) / math.sin(temperature * math.Pi)
    val secondPart: Double = inverseExponent(vertex.ord, exponent) / externalLinks
    val logged: Double = math.log(firstPart * secondPart)
    vertex.radial - (2 * logged)
  }
  // Connection probability.
  def probability(first: HyperVertex,
                  second: HyperVertex,
                  exponent: Double,
                  temperature: Double,
                  externalLinks: Double): Double = {
    val dist: Double = hyperbolicDistance(first, second)
    1 / (1 + math.exp((1 / (2 * temperature)) * (dist -
      expectedConnections(first, exponent, temperature, externalLinks))))
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
