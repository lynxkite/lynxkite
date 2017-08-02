// Gives vertices of a graph hyperbolic coordinates.
// These can later be used to evaluate edge strength or
// predict new links in the graph.
// Works on undirected graph.
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
    (j \ "temperature").as[Double],
    (j \ "seed").as[Long])
}
import HyperMap._
case class HyperMap(exponent: Double,
                    temperature: Double, seed: Long) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
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
    val degree = inputs.degree.rdd.map { case (id, degree) => (degree, id) }
    val avgExpectedDegree = degree.map { case (deg, id) => deg }.reduce(_ + _) / size
    val degreeOrdered = degree.sortBy(_._1, ascending = false).zipWithIndex.map {
      case ((degree, id), ord) =>
        (degree, id, ord + 1)
    }
    // Attempt to infer exponent by drawing a log-log plot line between the
    // highest-degree vertex and the lowest-degree vertices.
    val highestDegree = degreeOrdered.first._1
    val bottomDegree = degree.filter { case (deg, id) => deg != 0 }.sortBy(_._1, ascending = true).first._1
    val bottomCount = degreeOrdered.filter { case (deg, id, ord) => deg == bottomDegree }.count
    val gamma = math.log(bottomCount) / (math.log(highestDegree) - math.log(bottomDegree))
    // If it's outside the range of usual scale-free graphs, set it to a base value.
    val exponent = {
      if (2 < gamma && gamma < 3) 1 / (gamma - 1)
      else 0.6
    }
    // Collect ~log(n) samples to compare to, with earlier vertices being more likely.
    val collectedSamples = degreeOrdered.mapPartitionsWithIndex {
      case (pid, iter) =>
        val rnd = new Random((pid << 16) + seed)
        iter.filter {
          case (degree, id, ordinal) => rnd.nextDouble * ordinal < math.log(ordinal) ||
            ordinal < 3
        }
    }.collect.toList
    // Place down the first vertex with a random angular coordinate.
    val rndFirstVertex = new Random(seed)
    val firstSampleList = new HyperVertex(
      id = collectedSamples.head._2,
      ord = collectedSamples.head._3,
      radial = 2 * math.log(collectedSamples.head._3 * 2),
      angular = 2 * math.Pi * rndFirstVertex.nextDouble,
      expectedDegree = 0) :: Nil
    // Get the edges for building the remainder of sampleList.
    val collectedEdges = noLoopEdges.collect.toList
    val firstEdgesToSamples = collectedEdges.filter { case (id, e) => firstSampleList.head.id == e.dst }
    // Place down the rest of vertices in sampleList and get edges to them for later vertices.
    val sampleTuple: (List[HyperVertex], List[(Long, Edge)]) =
      collectedSamples.tail.foldLeft(firstSampleList, firstEdgesToSamples) {
        case ((currentSampleList, currentEdgesToSamples), currentSample) =>
          (HyperVertex(id = currentSample._2,
            ord = currentSample._3,
            radial = 2 * math.log(currentSample._3 * 2),
            angular = maximumLikelihoodAngular(currentSample._2, currentSample._3,
              currentSampleList, currentEdgesToSamples, exponent,
              temperature, avgExpectedDegree, logSize),
            expectedDegree = 0) :: currentSampleList,
            collectedEdges.filter { case (id, e) => currentSample._2 == e.dst } ++
            currentEdgesToSamples)
      }
    val sampleList = sampleTuple._1
    val edgesToSamples = sampleTuple._2
    val sampleVertexIDs = sampleList.map(vertex => vertex.id)
    // Place down the rest of the vertices simultaneously.
    val hyperVertices = degreeOrdered.map {
      case (degree, id, ord) =>
        HyperVertex(
          id = id,
          ord = ord,
          radial = 2 * math.log(ord),
          angular = maximumLikelihoodAngular(id, ord, sampleList,
            edgesToSamples, exponent, temperature, avgExpectedDegree, logSize),
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
    val radial = 2 * math.log(ord)
    samples.filter { v => v.ord < ord }
    samples.foldLeft(1.0)((product, otherVertex) =>
      if (!sampleEdges.filter { case (id, e) => e.src == vertexID && e.dst == otherVertex.id }.isEmpty) {
        product * probability(radial, otherVertex.radial, angular, otherVertex.angular,
          ord, exponent, temperature, avgExpectedDegree)
      } else {
        product * (1 - probability(radial, otherVertex.radial, angular, otherVertex.angular,
          ord, exponent, temperature, avgExpectedDegree))
      })
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
    val iterations: Int = (math.ceil(logSize)).toInt + 3
    val firstcwBound: Double = math.Pi * 2
    val firstccwBound: Double = 0
    val localRandom = new Random((vertexID << 16) + seed)
    val offset: Double = math.Pi * 2 * Random.nextDouble
    maximumLikelihoodRecursion(iterations,
      firstcwBound, firstccwBound, offset,
      vertexID, ord, samples, sampleEdges,
      exponent, temperature, avgExpectedDegree, logSize)
  }
  @annotation.tailrec
  private final def maximumLikelihoodRecursion(remainingIterations: Int,
                                               cwBound: Double,
                                               ccwBound: Double,
                                               offset: Double,
                                               vertexID: Long,
                                               ord: Long,
                                               samples: List[HyperVertex],
                                               sampleEdges: List[(Long, Edge)],
                                               exponent: Double,
                                               temperature: Double,
                                               avgExpectedDegree: Double,
                                               logSize: Double): Double = {
    val angleBound: Double = cwBound - ccwBound
    val topQuarterPoint: Double = cwBound - angleBound / 4
    val bottomQuarterPoint: Double = ccwBound + angleBound / 4
    val topValue: Double = likelihood(vertexID, ord, normalizeAngular(topQuarterPoint + offset),
      samples, sampleEdges, exponent, temperature, avgExpectedDegree)
    val bottomValue: Double = likelihood(vertexID, ord,
      normalizeAngular(bottomQuarterPoint + offset),
      samples, sampleEdges, exponent, temperature, avgExpectedDegree)
    val newcwBound = {
      if (topValue > bottomValue) cwBound
      else ccwBound + angleBound / 2
    }
    val newccwBound = {
      if (topValue > bottomValue) cwBound - angleBound / 2
      else ccwBound
    }
    val maxAngular = {
      if (topValue > bottomValue) normalizeAngular(topQuarterPoint + offset)
      else normalizeAngular(bottomQuarterPoint + offset)
    }
    if (remainingIterations == 0) maxAngular
    else maximumLikelihoodRecursion(remainingIterations - 1,
      newcwBound, newccwBound, offset,
      vertexID, ord, samples, sampleEdges,
      exponent, temperature, avgExpectedDegree, logSize)
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
