// Generates scale-free graph based on probability x similarity model.
// The degree distribution of the resulting graph will be scale-free and
// it will have high average clustering.
// Based on paper: https://www.caida.org/publications/papers/2015/network_mapping_replaying_hyperbolic/network_mapping_replaying_hyperbolic.pdf
package com.lynxanalytics.biggraph.graph_operations

import scala.math
import scala.util.Random
import scala.collection.mutable.PriorityQueue
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PSOGenerator extends OpFromJson {

  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {

    val (vs, es) = graph
    val radial = vertexAttribute[Double](vs)
    val angular = vertexAttribute[Double](vs)
  }
  def fromJson(j: JsValue) = PSOGenerator(
    (j \ "size").as[Long],
    (j \ "externalDegree").as[Int],
    (j \ "internalDegree").as[Int],
    (j \ "exponent").as[Double],
    (j \ "seed").as[Long])
}
import PSOGenerator._
case class PSOGenerator(size: Long, externalDegree: Int, internalDegree: Int, exponent: Double,
                        seed: Long) extends TypedMetaGraphOp[NoInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new NoInput

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj(
    "size" -> size,
    "externalDegree" -> externalDegree,
    "internalDegree" -> internalDegree,
    "exponent" -> exponent,
    "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = rc.partitionerForNRows(size)
    val ordinals = rc.sparkContext.parallelize(0L until size,
      partitioner.numPartitions).randomNumbered(partitioner)
    val sc = rc.sparkContext
    val masterRandom = new Random(seed)
    val logSize: Double = math.log(size.toDouble)
    // Adds the necessary attributes for later calculations.
    // reorderedID needs to be 1-indexed as log(0) will break things
    val reorderedID = ordinals.map { case (key, ordinal) => (key, ordinal + 1) }
    val radialAdded = reorderedID.map {
      case (key, reID) =>
        (key, (reID, 2 * math.log(reID.toDouble)))
    }
    val radial = radialAdded.map { case (key, (reID, radial)) => (key, radial) }
    val angularAdded = radialAdded.map({
      case (key, (reID, rad)) =>
        (key, (reID, rad, masterRandom.nextDouble * math.Pi * 2))
    })
    val angular = angularAdded.map { case (key, (reID, radial, angular)) => (key, angular) }
    val expectedDegree = angularAdded.map {
      case (key, (reID, rad, ang)) => (key, (reID, rad, ang,
        totalExpectedEPSO(exponent, externalDegree, internalDegree, size, reID)))
    }
    case class HyperVertex(key: Long, 
                           reorderedID: Long, 
                           radial: Double, 
                           angular: Double, 
                           eDegree: Double)
    // Samples ~log(n) vertices with the smallest angular coordinate difference as well as
    // preceding appearance (abstraction for higher popularity vertex; smaller radial coordinate)
    // Groups the samples for each vertex into a list. The first element of these are the vertices
    // then angular samples from cloclwise-most to counterclockwise-most, then radial samples.
    // Note: sample list will include the node itself in the middle.
    val allVerticesList: List[HyperVertex] = expectedDegree.map {
      case (key, (reID, rad, ang, eSam)) => HyperVertex(key, reID, rad, ang, eSam)
    }.collect().sortBy(_.angular).toList
    val possibilityList: List[List[(Long, Long, Double, Double, Double)]] = {
      val numFirstSamples: Int = (math.round(logSize * allVerticesList.head.eDegree)).toInt
      val endofVerticesList = allVerticesList.reverse.take(numFirstSamples)
      var resultList: List[List[HyperVertex]] = Nil
      var remainderList: List[HyperVertex] = allVerticesList
      var i = numFirstSamples
      while (i > 0 && !remainderList.isEmpty) {
        remainderList = remainderList.tail
        i -= 1
        } 
      var angularSampleList = endofVerticesList ++ allVerticesList.take(numFirstSamples)
      var radialSampleList = allVerticesList.head :: Nil
      resultList = (allVerticesList.head :: angularSampleList) :: resultList
      if (!allVerticesList.isEmpty) {
        for (vertex <- allVerticesList.tail) {
          val numCurrentSamples: Int = (math.round(logSize * vertex.eDegree)).toInt
          if (!radialSampleList.isEmpty) {
            radialSampleList = radialSampleList.take(numCurrentSamples)
          }
          radialSampleList = vertex :: radialSampleList

          // Once remainderList reaches the end makes it circular by sampling the beginning again.
          if (remainderList.isEmpty) remainderList = allVerticesList.take(numCurrentSamples)
          if (!angularSampleList.isEmpty) {
            angularSampleList = angularSampleList.take(numCurrentSamples * 2)
          }
          // Keeps the sample list centered on 'vertex'.
          if (numCurrentSamples >= (math.round(logSize * remainderList.head.eDegree)).toInt) {
            angularSampleList = remainderList.head :: angularSampleList
          }
          remainderList = remainderList.tail

          val vertexResult = vertex :: (angularSampleList ++ radialSampleList)
          resultList = vertexResult :: resultList
        }
      }
      resultList
    }
    // Selects the expectedDegree smallest distance edges from possibility bundles.
    val possibilities = sc.parallelize(possibilityList)
    val es = possibilities.map {
      case (data) =>
        var resultEdges: List[Edge] = Nil
        val numSelections: Int = data.head.eDegree.toInt
        val numSamples: Int = (math.round(logSize * data.eDegree)).toInt
        val src = data.head
        //TODO instead of a maxheap look into using rdd.top(numSelections)?
        // RDD orders by keys though. Make probability the key?
        val maxHeap = PriorityQueue.empty(Ordering.by[(Double, Long, Long), Double](_._1))
        def heapElement(src: HyperVertex,
                        dst: HyperVertex): (Double, (Long, Long)) = {
          (-hyperbolicDistance(src, dst), (src.key, dst.key))
        }
        //This could be parallelized and 'for' probably doesn't do it.
        if (!data.tail.isEmpty) {
          for (dstTuple <- data.tail) {
            if (srcTuple != dstTuple) maxHeap += heapElement(srcTuple, dstTuple)
          }
        }
        for (j <- 0 until numSelections) {
          val result = maxHeap.dequeue
          resultEdges = Edge(result._2, result._3) :: Edge(result._3, result._2) :: resultEdges
        }
        resultEdges
    }.flatMap(identity).distinct
    output(o.vs, ordinals.mapValues(_ => ()))
    output(o.radial, radial.sortUnique(partitioner))
    output(o.angular, angular.sortUnique(partitioner))
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
                              internalLinks: Int,
                              maxNodes: Long,
                              currentNodeID: Long): Double = {
    val firstPart: Double = ((2 * internalLinks.toDouble * (1 - exponent)) /
      (math.pow(1 - math.pow(maxNodes.toDouble, -(1 - exponent)), 2) * (2 * exponent - 1)))
    val secondPart: Double = math.pow((maxNodes / currentNodeID.toDouble), 2 * exponent - 1) - 1
    val thirdPart: Double = (1 - math.pow(currentNodeID.toDouble, -(1 - exponent)))
    firstPart * secondPart * thirdPart
  }
  // Expected number of connections at given time in the E-PSO model.
  def totalExpectedEPSO(exponent: Double,
                        externalLinks: Int,
                        internalLinks: Int,
                        maxNodes: Long,
                        currentNodeID: Long): Double = {
    externalLinks + internalConnectionsEPSO(exponent, internalLinks, maxNodes, currentNodeID)
  }
}
