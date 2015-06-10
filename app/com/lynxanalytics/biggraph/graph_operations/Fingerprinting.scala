// Operations for "fingerprinting": finding matching vertices by network structure.
package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

// For vertices on the two ends of "candidates" Fingerprinting will find the most likely match
// based on the network structure.
object Fingerprinting extends OpFromJson {
  class Input extends MagicInputSignature {
    val left = vertexSet
    val right = vertexSet
    val candidates = edgeBundle(left, right)
    val target = vertexSet
    val leftEdges = edgeBundle(left, target)
    val rightEdges = edgeBundle(right, target)
    val leftEdgeWeights = edgeAttribute[Double](leftEdges)
    val rightEdgeWeights = edgeAttribute[Double](rightEdges)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    // A subset of "candidates", which make up a strong matching.
    val matching = edgeBundle(
      inputs.left.entity, inputs.right.entity, EdgeBundleProperties.matching)
    // The similarity scores calculated for the matched vertices.
    val leftSimilarities = vertexAttribute[Double](inputs.left.entity)
    val rightSimilarities = vertexAttribute[Double](inputs.right.entity)
  }
  def fromJson(j: JsValue) =
    Fingerprinting((j \ "minimumOverlap").as[Int], (j \ "minimumSimilarity").as[Double])

  val maxIterations = 30
}
case class Fingerprinting(
  minimumOverlap: Int,
  minimumSimilarity: Double)
    extends TypedMetaGraphOp[Fingerprinting.Input, Fingerprinting.Output] {
  import Fingerprinting._
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("minimumOverlap" -> minimumOverlap, "minimumSimilarity" -> minimumSimilarity)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vertexPartitioner = inputs.target.rdd.partitioner.get
    val leftPartitioner = inputs.left.rdd.partitioner.get
    val rightPartitioner = inputs.right.rdd.partitioner.get

    // These are the two sides we are trying to connect.
    val lefts = inputs.candidates.rdd
      .map { case (_, e) => e.src -> () }
      .toSortedRDD(vertexPartitioner)
      .distinct
    val rights = inputs.candidates.rdd
      .map { case (_, e) => e.dst -> () }
      .toSortedRDD(vertexPartitioner)
      .distinct

    // Returns an RDD that maps each source vertex of "es" to a list of their neigbors.
    // With each neighbor the edge weight and the neighbor's in-degree are also included.
    def outNeighbors(
      es: EdgeBundleRDD,
      weights: AttributeRDD[Double]): AttributeRDD[Iterable[(ID, (Double, Double))]] = {
      val weightedEdges = es.sortedJoin(weights)
      val inDegrees = weightedEdges
        .map { case (_, (e, w)) => e.dst -> w }
        .reduceBySortedKey(vertexPartitioner, _ + _)
      weightedEdges
        .map { case (_, (e, w)) => e.dst -> (w, e.src) }
        .join(inDegrees)
        .map { case (dst, ((w, src), dstInDegree)) => src -> (dst, (w, dstInDegree)) }
        .groupBySortedKey(vertexPartitioner)
    }

    // Get the list of neighbors and their degrees for all candidates pairs.
    val candidates = inputs.candidates.rdd
      .map { case (_, e) => (e.dst, e.src) }
      .toSortedRDD(vertexPartitioner)
      .sortedLeftOuterJoin(outNeighbors(inputs.rightEdges.rdd, inputs.rightEdgeWeights.rdd))
      .map {
        case (rightID, (leftID, Some(rightNeighbors))) => (leftID, (rightID, rightNeighbors))
        case (rightID, (leftID, None)) => (leftID, (rightID, Seq()))
      }
      .toSortedRDD(vertexPartitioner)
      .sortedLeftOuterJoin(outNeighbors(inputs.leftEdges.rdd, inputs.leftEdgeWeights.rdd))
      .map {
        case (leftID, ((rightID, rightNeighbors), Some(leftNeighbors))) =>
          (leftID, leftNeighbors, rightID, rightNeighbors)
        case (leftID, ((rightID, rightNeighbors), None)) =>
          (leftID, Seq(), rightID, rightNeighbors)
      }

    // Calculate the similarity metric.
    val leftSimilarities =
      candidates.flatMap {
        case (leftID, leftNeighbors, rightID, rightNeighbors) =>
          val ln = leftNeighbors.toMap
          val rn = rightNeighbors.toMap
          val common = (ln.keySet intersect rn.keySet).toSeq
          if (common.isEmpty) {
            // Not just a shortcut. The formula would divide by zero if ln and rn are both empty.
            if (minimumOverlap > 0 || minimumSimilarity > 0) None
            else Some(leftID -> (rightID, 0.0))
          } else if (common.size < minimumOverlap) {
            None
          } else {
            val all = (ln.keySet union rn.keySet).toSeq
            // Weights.
            val lw = ln.mapValues(_._1).withDefaultValue(0.0)
            val rw = rn.mapValues(_._1).withDefaultValue(0.0)
            // Degrees.
            val ld = ln.mapValues(_._2.toDouble)
            val rd = rn.mapValues(_._2.toDouble)
            val degrees = all.map(k => k -> (ld.get(k) ++ rd.get(k))).toMap
            val avg = degrees.mapValues(ds => ds.sum / ds.size)
            // Calculate similarity score.
            val isect = common.map(k => (lw(k) min rw(k)) / avg(k)).sum
            val union = all.map(k => (lw(k) max rw(k)) / avg(k)).sum
            val similarity = isect / union
            if (similarity < minimumSimilarity) None
            else Some(leftID -> (rightID, similarity))
          }
      }.toSortedRDD(leftPartitioner)
    val rightSimilarities =
      leftSimilarities.map { case (l, (r, s)) => (r, (l, s)) }.toSortedRDD(rightPartitioner)

    // Run findStableMarriage with the smaller side as "ladies".
    def flipped(rdd: RDD[(ID, ID)]) =
      rdd.map(pair => pair._2 -> pair._1).toSortedRDD(vertexPartitioner)
    val leftToRight =
      if (rights.count < lefts.count)
        findStableMarriage(rights, lefts, rightSimilarities, leftSimilarities)
      else
        flipped(findStableMarriage(lefts, rights, leftSimilarities, rightSimilarities))
    output(o.matching, leftToRight.map {
      case (src, dst) => Edge(src, dst)
    }.randomNumbered(vertexPartitioner))
    output(o.leftSimilarities, leftSimilarities.sortedJoin(leftToRight).flatMapValues {
      case ((simID, sim), id) if simID == id => Some(sim)
      case _ => None
    })
    output(o.rightSimilarities, rightSimilarities.sortedJoin(flipped(leftToRight)).flatMapValues {
      case ((simID, sim), id) if simID == id => Some(sim)
      case _ => None
    })
  }

  // "ladies" is the smaller set. Returns a mapping from "gentlemen" to "ladies".
  def findStableMarriage(ladies: SortedRDD[ID, Unit],
                         gentlemen: SortedRDD[ID, Unit],
                         ladiesScores: SortedRDD[ID, (ID, Double)],
                         gentlemenScores: SortedRDD[ID, (ID, Double)]): SortedRDD[ID, ID] = {
    val partitioner = ladies.partitioner.get
    val gentlemenPreferences = gentlemenScores.groupByKey.mapValues {
      case ladies => ladies.sortBy(-_._2).map(_._1)
    }
    val ladiesPreferences = ladiesScores.groupByKey.mapValues {
      case gentlemen => gentlemen.sortBy(-_._2).map(_._1)
    }

    @annotation.tailrec
    def iterate(
      gentlemenCandidates: SortedRDD[ID, ArrayBuffer[ID]], iteration: Int): SortedRDD[ID, ID] = {

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
      } else if (iteration >= maxIterations) {
        // Reached maximal number of iterations. We don't try anymore.
        log.info(s"Fingerprinting reached maximal iteration count $maxIterations. Stopping.")
        responsesByGentlemen
      } else {
        iterate(
          gentlemenCandidates.sortedLeftOuterJoin(responsesByGentlemen).mapValues {
            case (ladies, Some(response)) => ladies // The proposal was accepted. Sit tight.
            case (ladies, None) => ladies.drop(1) // Rejected. Try the next lady.
          },
          iteration + 1)
      }
    }

    iterate(gentlemenPreferences, 1)
  }
}

// Generates the list of candidate matches for Fingerprinting. The vertices where "leftName" is
// defined but "rightName" is not are candidates for matching against vertices where "rightName" is
// defined but "leftName" is not, if the two vertices share at least one out-neighbor.
object FingerprintingCandidates extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val es = edgeBundle(vs, vs)
    val leftName = vertexAttribute[String](vs)
    val rightName = vertexAttribute[String](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val candidates = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = FingerprintingCandidates()
}
case class FingerprintingCandidates()
    extends TypedMetaGraphOp[FingerprintingCandidates.Input, FingerprintingCandidates.Output] {
  import FingerprintingCandidates._
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vertexPartitioner = inputs.vs.rdd.partitioner.get
    val edges = inputs.es.rdd.filter { case (_, e) => e.src != e.dst }
    val outEdges = edges.map { case (_, e) => e.src -> e.dst }.toSortedRDD(vertexPartitioner)

    // Returns the lines from the first attribute where the second attribute is undefined.
    def definedUndefined(defined: SortedRDD[ID, String],
                         undefined: SortedRDD[ID, String]): SortedRDD[ID, String] = {
      defined.sortedLeftOuterJoin(undefined).flatMapValues {
        case (_, Some(_)) => None
        case (name, None) => Some(name)
      }
    }
    val lefts = definedUndefined(inputs.leftName.rdd, inputs.rightName.rdd)
    val rights = definedUndefined(inputs.rightName.rdd, inputs.leftName.rdd)

    val leftNeighbors = outEdges
      .sortedJoin(lefts)
      .map { case (left, (mid, name)) => mid -> left }
    val rightNeighbors = outEdges
      .sortedJoin(rights)
      .map { case (right, (mid, name)) => mid -> right }
    val candidates = leftNeighbors
      .join(rightNeighbors)
      .values.distinct

    output(o.candidates,
      candidates
        .map { case (left, right) => Edge(left, right) }
        .randomNumbered(inputs.es.rdd.partitioner.get))
  }
}
