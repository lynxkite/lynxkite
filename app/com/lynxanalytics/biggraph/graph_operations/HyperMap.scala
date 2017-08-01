// Gives vertices of a graph hyperbolic coordinates.
// These can later be used to evaluate edge strength or
// predict new links in the graph.
// Works on undirected graph.
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

object HyperMap extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val degree = vertexAttribute[Double](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val radial = vertexAttribute[Double](inputs.vs.entity)
    val angular = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = HyperMap(
    (j \ "avgexpecteddegree").as[Double],
    (j \ "exponent").as[Double],
    (j \ "temperature").as[Double],
    (j \ "seed").as[Long])
}
import HyperMap._
case class HyperMap(avgExpectedDegree: Double, exponent: Double,
                    temperature: Double, seed: Long) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "avgexpecteddegree" -> avgExpectedDegree,
    "exponent" -> exponent,
    "temperature" -> temperature,
    "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vertices = inputs.vs.rdd
    val edges = inputs.es.rdd
    val sc = rc.sparkContext
    val size = inputs.vs.data.count.getOrElse(inputs.vs.rdd.count)
    // "log" used by scala.math is log_e. It can also do log_10 but that would be too few samples.
    val logSize = math.log(size)
    val vertexPartitioner = vertices.partitioner.get
    val edgePartitioner = edges.partitioner.get
    // Order vertices by descending degree to place higher-degree vertices first.
    val noLoopEdges = edges.filter { case (id, e) => e.src != e.dst }
    val degree = inputs.degree.rdd
    val degreeOrdered = degree.sortBy(_._2, false).zipWithIndex.map {
      case ((id, degree), ord) =>
        (id, degree, ord + 1)
    }
    // Collect ~log(n) samples to compare to, with earlier vertices being more likely.
    val collectedSamples = degreeOrdered.mapPartitionsWithIndex {
      case (pid, iter) =>
        val rnd = new Random((pid << 16) + seed)
        iter.filter {
          case (id, degree, ordinal) => rnd.nextDouble * ordinal < math.log(ordinal) ||
            ordinal < 3
        }
    }.collect.toList
    // Place down the first vertex with a random angular coordinate.
    val rndFirstVertex = new Random(seed)
    var sampleList = new HyperVertex(
      id = collectedSamples.head._1,
      ord = collectedSamples.head._3,
      radial = 2 * math.log(collectedSamples.head._3 * 2),
      angular = 2 * math.Pi * rndFirstVertex.nextDouble,
      expectedDegree = 0) :: Nil
    // Get the edges for building the remainder of sampleList.
    val collectedEdges = noLoopEdges.collect.toList
    var edgeListForSamples = collectedEdges.filter { case (id, e) => sampleList.head.id == e.dst }
    // Place down the rest of vertices in sampleList and get edges to them for later vertices.
    for (currentSample <- collectedSamples.tail) {
      val newHyperVertex = HyperVertex(
        id = currentSample._1,
        ord = currentSample._3,
        radial = 2 * math.log(currentSample._3 * 2),
        angular = maximumLikelihoodAngular(currentSample._1, currentSample._3, sampleList,
          edgeListForSamples, exponent, temperature, avgExpectedDegree, logSize),
        expectedDegree = 0)
      sampleList = newHyperVertex :: sampleList
      edgeListForSamples = collectedEdges.filter { case (id, e) => sampleList.head.id == e.dst } ++
        edgeListForSamples
    }
    // Broadcast the sample vertex list for fast comparisons.
    val bcSampleList = sc.broadcast(sampleList)
    val sampleVertexIDs = bcSampleList.value.map(vertex => vertex.id)
    // Broadcast the edges that go to a vertex in sampleList so that
    // checking whether two vertices are connected will take less time.
    val bcEdgesToSamples = sc.broadcast(edgeListForSamples)
    // Place down the rest of the vertices simultaneously.
    val hyperVertices = degreeOrdered.map {
      case (id, degree, ord) =>
        HyperVertex(
          id = id,
          ord = ord,
          radial = 2 * math.log(ord),
          angular = maximumLikelihoodAngular(id, ord, bcSampleList.value,
            bcEdgesToSamples.value, exponent, temperature, avgExpectedDegree, logSize),
          expectedDegree = 0)
    }

    output(o.radial, hyperVertices.map { v => (v.id, v.radial) }.sortUnique(vertexPartitioner))
    output(o.angular, hyperVertices.map { v => (v.id, v.angular) }.sortUnique(vertexPartitioner))
  }

  // Calculates the likelihood function for a node with a given angular coordinate.
  def likelihood(vertexID: Long,
                 ord: Long,
                 angular: Double,
                 samples: List[HyperVertex],
                 sampleEdges: List[(Long, Edge)],
                 exponent: Double,
                 temperature: Double,
                 avgExpectedDegree: Double): Double = {
    var product: Double = 1
    val radial = 2 * math.log(ord)
    for (otherVertex <- samples if otherVertex.ord < ord) {
      val prob: Double = probability(radial, otherVertex.radial, angular, otherVertex.angular,
        ord, exponent, temperature, avgExpectedDegree)
      if (prob != 1 && prob != 0) {
        if (!sampleEdges.filter { case (id, e) => e.src == vertexID && e.dst == otherVertex.id }.isEmpty) {
          product *= prob
        } else {
          product *= (1 - prob)
        }
      }
    }
    product
  }
  // Returns the optimal angular coordinate for a node.
  // Calculates the likelihood that the mapped graph is similar to a PSO-grown graph.
  // Divides 0 - 2Pi ( + offset) in half. Takes center point /random point of each, does a 
  // comparison. Higher one stays. Divides half as wide angle into two halves again and repeat.
  def maximumLikelihoodAngular(vertexID: Long,
                               ord: Long,
                               samples: List[HyperVertex],
                               sampleEdges: List[(Long, Edge)],
                               exponent: Double,
                               temperature: Double,
                               avgExpectedDegree: Double,
                               logSize: Double): Double = {
    var i: Int = (math.ceil(logSize)).toInt + 3
    var cwBound: Double = math.Pi * 2
    var ccwBound: Double = 0
    var maxAngular: Double = 0
    val localRandom = new Random((vertexID << 16) + seed)
    val offset: Double = math.Pi * 2 * Random.nextDouble
    while (i > 0) {
      val angleBound: Double = cwBound - ccwBound
      val topQuarterPoint: Double = cwBound - angleBound / 4
      val bottomQuarterPoint: Double = ccwBound + angleBound / 4
      val topValue: Double = likelihood(vertexID, ord, normalizeAngular(topQuarterPoint + offset),
        samples, sampleEdges, exponent, temperature, avgExpectedDegree)
      val bottomValue: Double = likelihood(vertexID, ord,
        normalizeAngular(bottomQuarterPoint + offset),
        samples, sampleEdges, exponent, temperature, avgExpectedDegree)
      if (topValue > bottomValue) {
        maxAngular = normalizeAngular(topQuarterPoint + offset)
        ccwBound = cwBound - angleBound / 2
      } else {
        maxAngular = normalizeAngular(bottomQuarterPoint + offset)
        cwBound = ccwBound + angleBound / 2
      }
      i -= 1
    }
    maxAngular
  }
  def normalizeAngular(ang: Double): Double = {
    if (ang > math.Pi * 2) ang - math.Pi * 2
    else ang
  }
  // Returns hyperbolic distance.
  def hyperbolicDistance(rad1: Double, rad2: Double, ang1: Double, ang2: Double): Double = {
    rad1 + rad2 + 2 * math.log(phi(ang1, ang2) / 2)
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
  def expectedConnections(rad1: Double,
                          ord: Long,
                          exponent: Double,
                          temperature: Double,
                          externalLinks: Double): Double = {
    val firstPart: Double = (2 * temperature) / math.sin(temperature * math.Pi)
    val secondPart: Double = inverseExponent(ord, exponent) / externalLinks
    val logged: Double = math.log(firstPart * secondPart)
    rad1 - (2 * logged)
  }
  // Connection probability.
  def probability(rad1: Double,
                  rad2: Double,
                  ang1: Double,
                  ang2: Double,
                  ord: Long,
                  exponent: Double,
                  temperature: Double,
                  externalLinks: Double): Double = {
    val dist: Double = hyperbolicDistance(rad1, rad2, ang1, ang2)
    1 / (1 + math.exp((1 / (2 * temperature)) * (dist -
      expectedConnections(rad1, ord, exponent, temperature, externalLinks))))
  }
}
