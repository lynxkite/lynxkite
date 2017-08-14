// Predicts edges in a graph that has hyperbolic coordinate attributes.
// Runs PSOGenerator with the already existing coordinates.
// Takes the top X most likely edges.
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
    val vs = vertexSet
    val radial = vertexAttribute[Double](vs)
    val angular = vertexAttribute[Double](vs)
  }
  class Output(
      implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val predictedEdges = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val edgeProbability = edgeAttribute[Double](predictedEdges)
  }
  def fromJson(j: JsValue) = HyperbolicPrediction(
    (j \ "size").as[Int],
    (j \ "externaldegree").as[Double],
    (j \ "internaldegree").as[Double],
    (j \ "exponent").as[Double])
}
import HyperbolicPrediction._
case class HyperbolicPrediction(size: Int, externalDegree: Double, internalDegree: Double,
                                exponent: Double) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "size" -> size,
    "externaldegree" -> externalDegree,
    "internaldegree" -> internalDegree,
    "exponent" -> exponent)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = inputs.vs.rdd.partitioner.get
    val vertexSetSize = inputs.vs.data.count.getOrElse(inputs.vs.rdd.count)
    val sc = rc.sparkContext
    val logVertexSetSize = math.log(vertexSetSize)
    val ordinals = inputs.radial.rdd.sortedLeftOuterJoin(inputs.angular.rdd)
      .map { case (id, (rad, ang)) => (id, rad, ang.getOrElse(0.0)) }
      .sortBy(_._2)
      .zipWithIndex
      .map { case ((id, rad, ang), ord) => (id, rad, ang, ord + 1) }
    val vertices = ordinals.map {
      case (id, radi, angu, ordi) =>
        HyperVertex(
          id = id,
          ord = ordi,
          radial = radi,
          angular = angu,
          expectedDegree = HyperDistance.totalExpectedEPSO(exponent,
            externalDegree, internalDegree, vertexSetSize, ordi))
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
        HyperDistance.linkedListSampler((logVertexSetSize * lhv.vertex.expectedDegree).toInt,
          lhv, lhv.previous, lhv.next, lhv.radialPrevious, Nil)
    }
    // Selects the expectedDegree smallest distance edges from possibility bundles.
    // Takes the $size most probable edges from all generated edges and adds them.
    // Temperature does not influence which edges PSOGenerator creates.
    // It only scales the probability numbers between 1 and 0. 0.45 is arbitrary but
    // produces sensible numbers, and exposing temperature could be confusing for the user.
    val possibilities = sc.parallelize(possibilityList)
    val extraEdges = possibilities.flatMap {
      data =>
        val numSelections: Int = data.head.expectedDegree.toInt
        val src = data.head
        val dst = data.tail.map {
          dst =>
            (HyperDistance.probability(src, dst, exponent, 0.45, externalDegree),
              Edge(src.id, dst.id))
        }.sortBy(-_._1)
        dst.take(numSelections)
    }.top(size)(ProbabilityOrdering)
    val extraEdgesRDD = sc.parallelize(extraEdges)
    val predictedEdges = extraEdgesRDD.flatMap {
      case (probability, edge) =>
        List((edge, probability), (Edge(edge.dst, edge.src), probability))
    }
    val randomNumberedEdges = predictedEdges.randomNumbered(partitioner)
    output(o.predictedEdges, randomNumberedEdges.map {
      case (id, (e, prob)) =>
        (id, e)
    }.sortUnique(partitioner))
    output(o.edgeProbability, randomNumberedEdges.map {
      case (id, (e, prob)) =>
        (id, prob)
    }.sortUnique(partitioner))
  }
}
object ProbabilityOrdering extends Ordering[(Double, Edge)] {
  def compare(a: (Double, Edge), b: (Double, Edge)) = a._1 compare b._1
}
