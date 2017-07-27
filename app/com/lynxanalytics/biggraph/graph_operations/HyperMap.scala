// Gives vertices of a graph hyperbolic coordinates
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

object HyperMap extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vertices, edges) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val radial = vertexAttribute[Double](inputs.vertices.entity)
    val angular = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = HyperMap(
    (j \ "avgExpectedDegree").as[Int],
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
    "avgExpectedDegree" -> avgExpectedDegree,
    "exponent" -> exponent,
    "temperature" -> temperature,
    "seed" -> seed)

  //TODO avgExpectedDegree automatic setting has been left out
  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vs = inputs.vertices.rdd
    val es = inputs.edges.rdd
    val size = inputs.vertices.data.count.getOrElse(inputs.vertices.rdd.count)
    val logSize = math.log(size)
    val vertexPartitioner = vs.partitioner.get
    val edgePartitioner = es.partitioner.get

    val filteredEdges = es.filter { case (id, e) => e.src != e.dst }
    val degreeWithoutIsolatedVertices = filteredEdges.flatMap {
      case (id, e) => Seq(e.src -> 1.0, e.dst -> 1.0)
    }.reduceBySortedKey(vertexPartitioner, _ + _)
    //val dwiw = degreeWithoutIsolatedVertices.sortUnique
    val degree = vs.sortedLeftOuterJoin(degreeWithoutIsolatedVertices).
      mapValues(_._2.getOrElse(0.0))

    val degreeOrdered = degree.sortBy(_._2, false).zipWithIndex.map {
      case ((id, degree), ord) =>
        (id, degree, ord + 1)
    }
    val collectedSamples = degreeOrdered.mapPartitionsWithIndex {
      case (pid, iter) =>
        val rnd = new Random((pid << 16) + seed)
        iter.filter {
          case (id, degree, ordinal) => rnd.nextDouble * ordinal < math.log(ordinal) ||
            ordinal < 3
        }
    }.collect.toList
    val sampleList: List[HyperVertex] = collectedSamples.map { currentSample =>
      HyperVertex(
        id = currentSample._1,
        ord = currentSample._3,
        radial = 2 * math.log(currentSample._3 * 2),
        angular = maximumLikelihoodAngular(currentSample._3,
          sampleList, exponent, temperature, avgExpectedDegree, logSize),
        expectedDegree = 0)
    }
    val hyperVertices = degreeOrdered.map {
      case (id, degree, ord) =>
        HyperVertex(
          id = id,
          ord = ord,
          radial = 2 * math.log(ord),
          angular = maximumLikelihoodAngular(ord, sampleList,
            exponent, temperature, avgExpectedDegree, logSize),
          expectedDegree = 0)
    }

    output(o.radial, hyperVertices.map { v => (v.id, v.radial) }.sortUnique(vertexPartitioner))
    output(o.angular, hyperVertices.map { v => (v.id, v.angular) }.sortUnique(vertexPartitioner))
  }

  // Calculates the likelihood function for a node with a given angular coordinate.
  def likelihood(ord: Long,
                 angular: Double,
                 samples: List[HyperVertex],
                 exponent: Double,
                 temperature: Double,
                 avgExpectedDegree: Double): Double = {
    var product: Double = 1
    val radial = 2 * math.log(ord)
    for (otherVertex <- samples if otherVertex.ord < ord) {
      val prob: Double = probability(radial, otherVertex.radial, angular, otherVertex.angular, ord, exponent, temperature, avgExpectedDegree)
      if (prob != 1 && prob != 0) {
        //TODO how to replace this one below?
        if (currentNode.edgesTo.contains(otherNode)) {
          product *= prob
        } else {
          product *= (1 - prob)
        }
      }
    }
    product
  }
  // Returns the optimal angular coordinate for a node.
  // Calculates the likelihood that the mapped graph is similar to a PSO-grown graph, O(log n).
  // Divides 0 - 2Pi ( + offset) in half. Takes center point /random point of each, does a comparison. Higher one stays.
  // Divides half as wide angle into two halves again and repeat.
  def maximumLikelihoodAngular(ord: Long,
                               samples: List[HyperVertex],
                               exponent: Double,
                               temperature: Double,
                               avgExpectedDegree: Double,
                               logSize: Double): Double = {
    var i: Int = (math.ceil(logSize)).toInt + 3
    var cwBound: Double = math.Pi * 2
    var ccwBound: Double = 0
    var maxAngular: Double = 0
    while (i > 0) {
      val angleBound: Double = cwBound - ccwBound
      val topQuarterPoint: Double = cwBound - angleBound / 4
      val bottomQuarterPoint: Double = ccwBound + angleBound / 4
      val topValue: Double = likelihood(ord, topQuarterPoint, samples,
        exponent, temperature, avgExpectedDegree)
      val bottomValue: Double = likelihood(ord, bottomQuarterPoint, samples,
        exponent, temperature, avgExpectedDegree)
      if (topValue > bottomValue) {
        maxAngular = topQuarterPoint
        ccwBound = cwBound - angleBound / 2
      } else {
        maxAngular = bottomQuarterPoint
        cwBound = ccwBound + angleBound / 2
      }
      i -= 1
    }
    maxAngular
  }
  // Returns hyperbolic distance.
  def hyperbolicDistance(rad1: Double, rad2: Double, ang1: Double, ang2: Double): Double = {
    rad1 + rad2 + 2 * math.log(phi(ang1, ang2) / 2)
  }
  // Returns angular component for hyperbolic distance calculation.
  def phi(ang1: Double, ang2: Double): Double = {
    math.Pi - math.abs(math.Pi - math.abs(ang1 - ang2))
  }
  // Equation for parameter denoted I_i in the HyperMap paper.
  def inverseExponent(ord: Long, exponent: Double): Double = {
    (1 / (1 - exponent)) * (1 - math.pow(ord, -(1 - exponent)))
  }
  // Expected number of connections for a vertex.
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
