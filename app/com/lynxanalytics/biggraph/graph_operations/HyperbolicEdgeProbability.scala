// Gives edge attribute "hyperbolic_edge_probability" to all edges in a graph.
// Graph must have vertex attributes that can be used for "radial" and "angular"
package com.lynxanalytics.biggraph.graph_operations

import scala.math
import scala.util.Random
import scala.collection.immutable.SortedMap
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object HyperbolicEdgeProbability extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val radial = vertexAttribute[Double](vs)
    val angular = vertexAttribute[Double](vs)
    val degree = vertexAttribute[Double](vs)
    val clustering = vertexAttribute[Double](vs)
  }
  class Output(
      implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val edgeProbability = edgeAttribute[Double](inputs.es.entity)
  }
  def fromJson(j: JsValue) = HyperbolicEdgeProbability()

}
import HyperbolicEdgeProbability._
case class HyperbolicEdgeProbability() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val partitioner = edges.partitioner.get
    val sc = rc.sparkContext
    val vertexSetSize = inputs.vs.data.count.getOrElse(inputs.vs.rdd.count)
    // Attempts to infer temperature from clustering coefficient.
    val avgClustering = inputs.clustering.rdd.map { case (id, clus) => clus }
      .reduce(_ + _) / vertexSetSize
    val temperature = (1 - avgClustering) * 0.9
    // Attempts to infer exponent by drawing a log-log plot line between the
    // highest-degree vertex and the lowest-degree vertices.
    val degree = inputs.degree.rdd
    val avgExpectedDegree = degree.map { case (id, deg) => deg }.reduce(_ + _) / vertexSetSize.toDouble
    val degreeOrdered = degree.sortBy(_._2, ascending = false)
    val highestDegree = degreeOrdered.first._2
    val bottomDegree = degree.filter { case (id, deg) => deg != 0 }
      .sortBy(_._2, ascending = true).first._2
    val bottomCount = degreeOrdered.filter { case (id, deg) => deg == bottomDegree }.count
    val gamma = math.log(bottomCount) / (math.log(highestDegree) - math.log(bottomDegree))
    // If exponent is outside the range of usual scale-free graphs, set it to a base value.
    val exponent = {
      if (2 < gamma && gamma < 3) 1 / (gamma - 1)
      else 0.6
    }
    // Creates HyperVertices
    val ordinals = inputs.radial.rdd.sortedLeftOuterJoin(inputs.angular.rdd)
      .map { case (id, (rad, ang)) => (id, rad, ang.getOrElse(0.0)) }
      .sortBy(_._2)
      .zipWithIndex
      .map { case ((id, rad, ang), ord) => (id, rad, ang, ord + 1) }
    val vertices = ordinals.map {
      case (id, radi, angu, ordi) =>
        (id -> HyperVertex(
          id = id,
          ord = ordi,
          radial = radi,
          angular = angu,
          expectedDegree = 0))
    }.sortUnique(partitioner)
    val srcGrouped = edges.map { case (id, e) => (e.src -> (id, e)) }
      .groupBySortedKey(partitioner)
      .sortedLeftOuterJoin(vertices)
      .flatMap {
        case (id, (ideiterable, vertex)) =>
          ideiterable.map { case (id, e) => id -> vertex.get }
      }.sortUnique(partitioner)
    val dstGrouped = edges.map { case (id, e) => (e.dst -> (id, e)) }
      .groupBySortedKey(partitioner)
      .sortedLeftOuterJoin(vertices)
      .flatMap {
        case (id, (ideiterable, vertex)) =>
          ideiterable.map { case (id, e) => id -> vertex.get }
      }.sortUnique(partitioner)
    val edgesJoined = srcGrouped.sortedLeftOuterJoin(dstGrouped)
    val edgeProbability = edgesJoined.map {
      case (eid, (srcVertex, dstVertex)) =>
        (eid,
          probability(srcVertex, dstVertex.get, exponent, temperature, avgExpectedDegree))
    }
    output(o.edgeProbability, edgeProbability.sortUnique(partitioner))
  }
  // Returns hyperbolic distance.
  def hyperbolicDistance(src: HyperVertex, dst: HyperVertex): Double = {
    src.radial + src.radial + 2 * math.log(phi(src.angular, dst.angular) / 2)
  }
  // Returns angular component for hyperbolic distance calculation.
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
}

