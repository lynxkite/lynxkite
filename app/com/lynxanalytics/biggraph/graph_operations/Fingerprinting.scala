package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

object Fingerprinting {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val edgeIds = vertexSet
    val es = edgeBundle(vs, vs, idSet = edgeIds)
    val weight = vertexAttribute[Double](edgeIds)
    val name = vertexAttribute[String](vs)
    val side = vertexAttribute[String](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val leftName = vertexAttribute[String](inputs.vs.entity)
    val rightName = vertexAttribute[String](inputs.vs.entity)
  }
}
import Fingerprinting._
case class Fingerprinting(
  leftSide: String,
  rightSide: String,
  // TODO: There seems to be a mistake in the doc and MREW's role is unclear.
  minimumRelativeEdgeWeight: Double,
  minimumOverlap: Int,
  minimumSimilarity: Double)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd.filter { case (_, e) => e.src != e.dst }
    val vertices = inputs.vs.rdd
    val vertexPartitioner = vertices.partitioner.get
    val weightedEdges = edges.sortedJoin(inputs.weight.rdd)
    val inNeighbors = weightedEdges
      .map { case (_, (e, w)) => e.dst -> (w, e.src) }
      .groupBySortedKey(vertexPartitioner)
      .mapValues(it => collection.SortedSet(it.toSeq: _*).toArray)
    val inDegrees = inNeighbors.mapValues(_.map(_._1).sum)

    def byName[T](rdd: SortedRDD[ID, T]): SortedRDD[String, ArrayBuffer[(ID, T)]] = {
      val named = inputs.name.rdd.sortedJoin(rdd)
      named.map {
        case (id, (name, t)) => name -> (id, t)
      }.toSortedRDD(vertexPartitioner).groupByKey
    }
    def getSide(which: String): SortedRDD[ID, String] = {
      byName(inputs.side.rdd).flatMap {
        case (name, idSides) =>
          def id = idSides(0)._1
          def side = idSides(0)._2
          if (idSides.size == 1 && side == which) Some(id -> name) else None
      }.toSortedRDD(vertexPartitioner)
    }
    val lefts = getSide(leftSide)
    val rights = getSide(rightSide)

    val outNeighbors = weightedEdges
      .map { case (_, (e, w)) => e.dst -> (w, e.src) }
      .join(inDegrees)
      .join(inputs.name.rdd)
      .map { case (dst, (((w, src), deg), dstName)) => src -> (dstName, (w, deg)) }
      .groupBySortedKey(vertexPartitioner)

    // Calculate the similarity metric.
    val similarities =
      (lefts.sortedJoin(outNeighbors) cartesian rights.sortedJoin(outNeighbors)).flatMap {
        case ((leftID, (leftName, leftNeighbors)), (rightID, (rightName, rightNeighbors))) =>
          val ln = leftNeighbors.toMap
          val rn = rightNeighbors.toMap
          val common = ln.keySet intersect rn.keySet
          val all = ln.keySet union rn.keySet
          // Weights.
          val lw = ln.mapValues(_._1).withDefaultValue(0.0)
          val rw = rn.mapValues(_._1).withDefaultValue(0.0)
          // Degrees.
          val ld = ln.mapValues(_._2.toDouble)
          val rd = rn.mapValues(_._2.toDouble)
          val degrees = all.map(k => k -> (ld.get(k) ++ rd.get(k))).toMap
          val avg = degrees.mapValues(ds => ds.sum / ds.size)
          println(s"common: $common")
          if (common.size < minimumOverlap) {
            Iterator()
          } else {
            val isect = common.map(k => (lw(k) min rw(k)) / avg(k)).sum
            val union = common.map(k => (lw(k) max rw(k)) / avg(k)).sum
            println(s"sim($leftID, $rightID) = $isect / $union")
            val similarity = isect / union
            if (similarity < minimumSimilarity) None
            else Iterator(leftID -> (rightID, similarity), rightID -> (leftID, similarity))
          }
      }.toSortedRDD(vertexPartitioner)

    // Run stableMarriage with the smaller side as "ladies".
    def flipped(rdd: RDD[(ID, ID)]) =
      rdd.map(pair => pair._2 -> pair._1).toSortedRDD(vertexPartitioner)
    val (leftToRight, rightToLeft) =
      if (lefts.count < rights.count) {
        val rightToLeft = stableMarriage(lefts, rights, similarities)
        (flipped(rightToLeft), rightToLeft)
      } else {
        val leftToRight = stableMarriage(rights, lefts, similarities)
        (leftToRight, flipped(leftToRight))
      }

    // Map IDs to names in the output.
    def reverseNameMap(leftToRight: SortedRDD[ID, ID]) = {
      leftToRight.sortedJoin(inputs.name.rdd).map {
        case (leftID, (rightID, leftName)) => rightID -> leftName
      }.toSortedRDD(vertexPartitioner)
    }
    output(o.leftName, reverseNameMap(leftToRight))
    output(o.rightName, reverseNameMap(rightToLeft))
  }

  // "ladies" is the smaller set.
  def stableMarriage(ladies: SortedRDD[ID, String], gentlemen: SortedRDD[ID, String], preferences: SortedRDD[ID, (ID, Double)]): SortedRDD[ID, ID] = {
    println(s"ladies: ${ladies.collect.toSeq}")
    println(s"gentlemen: ${gentlemen.collect.toSeq}")
    println(s"preferences: ${preferences.collect.toSeq}")
    val ladiesCount = ladies.count
    val partitioner = ladies.partitioner.get
    val gentlemenPreferences = gentlemen.sortedJoin(preferences).mapValues(_._2).groupByKey.mapValues {
      case ladies => ladies.sortBy(-_._2).map(_._1)
    }
    val ladiesPreferences = ladies.sortedJoin(preferences).mapValues(_._2).groupByKey.mapValues {
      case gentlemen => gentlemen.sortBy(-_._2).map(_._1)
    }
    var gentlemenCandidates = gentlemenPreferences // The diminishing list of candidates.
    while (true) {
      println(s"gc: ${gentlemenCandidates.count}")
      val proposalsByLadies = gentlemenCandidates.flatMap {
        case (gentleman, ladies) =>
          println(s"gentleman: $gentleman, ladies: ${ladies.toSeq}")
          if (ladies.isEmpty) None else Some(ladies.head -> gentleman)
      }.groupBySortedKey(partitioner)
      val responsesByGentlemen = proposalsByLadies.sortedJoin(ladiesPreferences).map {
        case (lady, (proposals, preferences)) =>
          val ps = proposals.toSet
          // Preferences are symmetrical, so we will always find one here.
          preferences.find(g => ps.contains(g)).get -> lady
      }.toSortedRDD(partitioner)
      println(s"lc: $ladiesCount, rc: ${responsesByGentlemen.count}")
      if (ladiesCount == responsesByGentlemen.count) {
        // All the ladies are happily engaged. Stop iteration.
        return responsesByGentlemen
      } else {
        gentlemenCandidates = gentlemenCandidates.sortedLeftOuterJoin(responsesByGentlemen).mapValues {
          case (ladies, Some(response)) => ladies // The proposal was accepted. Sit tight.
          case (ladies, None) => ladies.drop(1) // Rejected. Try the next lady.
        }
      }
    }
    ???
  }
}
