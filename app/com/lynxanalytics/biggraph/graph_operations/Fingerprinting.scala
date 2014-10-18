package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

// For vertices that only have one of leftName/rightName Fingerprinting will find the most likely
// match based on networks structure. Nodes that have both leftName/rightName defined are used for
// finding the matches.
object Fingerprinting {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val edgeIds = vertexSet
    val es = edgeBundle(vs, vs, idSet = edgeIds)
    val weight = vertexAttribute[Double](edgeIds)
    val leftName = vertexAttribute[String](vs)
    val rightName = vertexAttribute[String](vs)
    // A helper edge bundle that restricts the search to a subset of the possible pairings.
    // It should point left to right.
    val candidates = edgeBundle(vs, vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val leftToRight = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
}
import Fingerprinting._
case class Fingerprinting(
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
    val vertexPartitioner = inputs.vs.rdd.partitioner.get

    // These are the two sides we are trying to connect.
    def definedUndefined(defined: SortedRDD[ID, String], undefined: SortedRDD[ID, String]): SortedRDD[ID, String] = {
      defined.sortedLeftOuterJoin(undefined).flatMapValues {
        case (_, Some(_)) => None
        case (name, None) => Some(name)
      }
    }
    val lefts = definedUndefined(inputs.leftName.rdd, inputs.rightName.rdd)
    val rights = definedUndefined(inputs.rightName.rdd, inputs.leftName.rdd)

    // Get the list of neighbors and their degrees for all candidates pairs.
    val edges = inputs.es.rdd.filter { case (_, e) => e.src != e.dst }
    val weightedEdges = edges.sortedJoin(inputs.weight.rdd)
    val inDegrees = weightedEdges
      .map { case (_, (e, w)) => e.dst -> w }
      .groupBySortedKey(vertexPartitioner)
      .mapValues(_.sum)
    val outNeighbors = weightedEdges
      .map { case (_, (e, w)) => e.dst -> (w, e.src) }
      .join(inDegrees)
      .map { case (dst, ((w, src), deg)) => src -> (dst, (w, deg)) }
      .groupBySortedKey(vertexPartitioner)
    val candidates = inputs.candidates.rdd
      .map { case (_, e) => (e.dst, e.src) }
      .toSortedRDD(vertexPartitioner)
      .sortedJoin(outNeighbors)
      .map { case (rightID, (leftID, rightNeighbors)) => (leftID, (rightID, rightNeighbors)) }
      .toSortedRDD(vertexPartitioner)
      .sortedJoin(outNeighbors)
      .map {
        case (leftID, ((rightID, rightNeighbors), leftNeighbors)) =>
          (leftID, leftNeighbors, rightID, rightNeighbors)
      }

    // Calculate the similarity metric.
    val similarities =
      candidates.flatMap {
        case (leftID, leftNeighbors, rightID, rightNeighbors) =>
          val ln = leftNeighbors.toMap
          val rn = rightNeighbors.toMap
          val common = (ln.keySet intersect rn.keySet).toSeq
          val all = (ln.keySet union rn.keySet).toSeq
          // Weights.
          val lw = ln.mapValues(_._1).withDefaultValue(0.0)
          val rw = rn.mapValues(_._1).withDefaultValue(0.0)
          // Degrees.
          val ld = ln.mapValues(_._2.toDouble)
          val rd = rn.mapValues(_._2.toDouble)
          val degrees = all.map(k => k -> (ld.get(k) ++ rd.get(k))).toMap
          val avg = degrees.mapValues(ds => ds.sum / ds.size)
          if (common.size < minimumOverlap) {
            Iterator()
          } else {
            val isect = common.map(k => (lw(k) min rw(k)) / avg(k)).sum
            val union = all.map(k => (lw(k) max rw(k)) / avg(k)).sum
            val similarity = isect / union
            if (similarity < minimumSimilarity) None
            else Iterator(leftID -> (rightID, similarity), rightID -> (leftID, similarity))
          }
      }.toSortedRDD(vertexPartitioner)

    // Run stableMarriage with the smaller side as "ladies".
    def flipped(rdd: RDD[(ID, ID)]) =
      rdd.map(pair => pair._2 -> pair._1).toSortedRDD(vertexPartitioner)
    val leftToRight =
      if (rights.count < lefts.count) stableMarriage(rights, lefts, similarities)
      else flipped(stableMarriage(lefts, rights, similarities))
    output(o.leftToRight, leftToRight.mapValuesWithKeys {
      case (src, dst) => Edge(src, dst)
    })
  }

  // "ladies" is the smaller set. Returns a mapping from "gentlemen" to "ladies".
  def stableMarriage(ladies: SortedRDD[ID, String], gentlemen: SortedRDD[ID, String], preferences: SortedRDD[ID, (ID, Double)]): SortedRDD[ID, ID] = {
    val partitioner = ladies.partitioner.get
    val gentlemenPreferences = preferences.sortedJoin(gentlemen).mapValues(_._1).groupByKey.mapValues {
      case ladies => ladies.sortBy(-_._2).map(_._1)
    }
    val ladiesPreferences = preferences.sortedJoin(ladies).mapValues(_._1).groupByKey.mapValues {
      case gentlemen => gentlemen.sortBy(-_._2).map(_._1)
    }

    @annotation.tailrec
    def iterate(gentlemenCandidates: SortedRDD[ID, ArrayBuffer[ID]]): SortedRDD[ID, ID] = {
      val proposals = gentlemenCandidates.flatMap {
        case (gentleman, ladies) =>
          if (ladies.isEmpty) None else Some(ladies.head -> gentleman)
      }
      val proposalsByLadies = proposals.groupBySortedKey(partitioner)
      val responsesByGentlemen = proposalsByLadies.sortedJoin(ladiesPreferences).map {
        case (lady, (proposals, preferences)) =>
          val ps = proposals.toSet
          // Preferences are symmetrical, so we will always find one here.
          preferences.find(g => ps.contains(g)).get -> lady
      }.toSortedRDD(partitioner)
      if (proposals.count == responsesByGentlemen.count) {
        // All proposals accepted. Stop iteration.
        responsesByGentlemen
      } else {
        iterate(gentlemenCandidates.sortedLeftOuterJoin(responsesByGentlemen).mapValues {
          case (ladies, Some(response)) => ladies // The proposal was accepted. Sit tight.
          case (ladies, None) => ladies.drop(1) // Rejected. Try the next lady.
        })
      }
    }

    iterate(gentlemenPreferences)
  }
}
