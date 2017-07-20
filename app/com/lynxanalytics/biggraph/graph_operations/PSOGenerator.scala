// Generates scale-free graph based on probability x similarity model
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
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val (vertices, edges) = graph
    val radial = vertexAttribute[Double](vertices)
    val angular = vertexAttribute[Double](vertices)
    //val reorderedID = vertexAttribute[Int](vertices)
    //val edgeProbability = edgeAttribute[Double](edges)
  }
  def fromJson(j: JsValue) = PSOGenerator(
    (j \ "externalDegree").as[Int],
    (j \ "internalDegree").as[Int],
    (j \ "exponent").as[Double],
    (j \ "temperature").as[Double],
    (j \ "seed").as[Long])
}
import PSOGenerator._
case class PSOGenerator(externalDegree: Int, internalDegree: Int,
                        exponent: Double, temperature: Double, seed: Long) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "externalDegree" -> externalDegree,
    "internalDegree" -> internalDegree,
    "exponent" -> exponent,
    "temperature" -> temperature,
    "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sc = rc.sparkContext
    val vs = inputs.vs.rdd
    val masterRandom = new Random(seed)
    val numVertices = vs.count
    val logNumVertices: Double = math.log(numVertices.toDouble)
    val reorderedID = vs.zipWithIndex.map { case ((key, nothing), reID) => (key, reID + 1) }
    val radialAdded = reorderedID.map { case (key, reID) => (key, (reID, 2 * math.log(reID.toDouble))) }
    val radial = radialAdded.map { case (key, (reID, radial)) => (key, radial) }
    val angularAdded = radialAdded.map({ case (key, (reID, rad)) => (key, (reID, rad, masterRandom.nextDouble * math.Pi * 2)) })
    val angular = angularAdded.map { case (key, (reID, radial, angular)) => (key, angular) }
    val expectedSamples = angularAdded.map {
      case (key, (reID, rad, ang)) => (key, (reID, rad, ang,
        (math.round(logNumVertices * totalExpectedEPSO(exponent, externalDegree, internalDegree, numVertices, reID))).toInt))
    }
    // key | reorderedID | radial | angular | (expectedDegree)
    // Groups the samples for each vertex into a list. The first element of these are the vertices, then 
    // angular samples from cloclwise-most to counterclockwise-most, then radial samples.
    // Note: sample list will include the node itself in the middle.
    val allVerticesList: List[(Long, Long, Double, Double, Int)] = expectedSamples.map {
      case (key, (reID, rad, ang, eSam)) => (key, reID, rad, ang, eSam)
    }.collect().toList
    val possibilityList: List[List[(Long, Long, Double, Double, Int)]] = {
      val numFirstSamples: Int = allVerticesList.head._5
      val endofVerticesList = allVerticesList.reverse.take(numFirstSamples)
      var resultList: List[List[(Long, Long, Double, Double, Int)]] = Nil
      var remainderList: List[(Long, Long, Double, Double, Int)] = allVerticesList
      var i = numFirstSamples
      while (i > 0) remainderList = remainderList.tail
      var angularSampleList: List[(Long, Long, Double, Double, Int)] = endofVerticesList ++ allVerticesList.take(numFirstSamples)
      var radialSampleList: List[(Long, Long, Double, Double, Int)] = allVerticesList.head :: Nil
      resultList = (allVerticesList.head :: angularSampleList) :: resultList
      for (vertex <- allVerticesList.tail) {
        if (!radialSampleList.isEmpty) radialSampleList = radialSampleList.take(vertex._5)
        radialSampleList = vertex :: radialSampleList

        if (remainderList.isEmpty) remainderList = allVerticesList.take(numFirstSamples)
        if (!angularSampleList.isEmpty) angularSampleList = angularSampleList.take(vertex._5 * 2)
        // Small modification if clause to keep the sample list centered on 'vertex'
        if (vertex._5 >= remainderList.head._5) angularSampleList = remainderList.head :: angularSampleList
        remainderList = remainderList.tail

        val vertexResult = vertex :: (angularSampleList ++ radialSampleList)
        resultList = vertexResult :: resultList
      }
      resultList
    }
    val possibilities = sc.parallelize(possibilityList)
    val edges = sc.parallelize(possibilities.map {
      case (data) =>
        {
          var resultEdges: List[(Long, Long)] = Nil
          val numSelections: Double = totalExpectedEPSO(exponent, externalDegree, internalDegree,
            numVertices, data.head._1)
          val numSamples: Int = (math.log(numVertices) * numSelections).toInt
          val srcTuple = data.head
          // hyperbolicDistance | src key | dst key
          //TODO instead of a maxheap look into using rdd.top(numSelections)?
          // How do you define a complex ordering for RDDs? It might only sort by keys.
          val maxHeap = PriorityQueue.empty(Ordering.by[(Double, Long, Long), Double](_._1))
          def heapElement(srcTuple: (Long, Long, Double, Double, Int),
                          dstTuple: (Long, Long, Double, Double, Int)): (Double, Long, Long) = {
            (-hyperbolicDistance(srcTuple._3, dstTuple._3, srcTuple._4, dstTuple._4),
              srcTuple._1, dstTuple._1)
          }
          //This technically could be parallelized and 'for' probably doesn't do it.
          for (dstTuple <- data.tail) {
            if (srcTuple != dstTuple) maxHeap += heapElement(srcTuple, dstTuple)
          }
          for (j <- 0 until numSelections.toInt) {
            val result = maxHeap.dequeue
            resultEdges = (result._2, result._3) :: resultEdges
          }
          resultEdges
        }
    }.reduce(_ ++ _).map { case (edge1, edge2) => Edge(edge1, edge2) })
    //TODO turn output things into lynxanalytics.EdgeBundleRDD
    // optional: discard parallel edges? EdgeSet?
    output(o.radial, radial.sortUnique(inputs.vs.rdd.partitioner.get))
    output(o.angular, angular.sortUnique(inputs.vs.rdd.partitioner.get))
    output(o.edges, edges.randomNumbered(inputs.vs.rdd.partitioner.get))
  }
  // Returns hyperbolic distance.
  def hyperbolicDistance(rad1: Double, rad2: Double, ang1: Double, ang2: Double): Double = {
    rad1 + rad2 + 2 * math.log(phi(ang1, ang2) / 2)
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
