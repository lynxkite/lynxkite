// Operations for "fingerprinting": finding matching vertices by network structure.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.HashPartitioner

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

// For vertices on the two ends of "candidates" Fingerprinting will find the most likely match
// based on the network structure.
object Fingerprinting extends OpFromJson {
  // TODO: These parameters are somewhat temporary until we figure out exactly what kind of
  // configuration we need for fingerpringing. Users cannot set these without explicit instructions
  // from us, so we don't have to take compatibility considerations too seriously about them.
  // (But if we do instruct someone to use these, they need to be warned.)
  private val weightingModeParameter = NewParameter("weightingMode", "InverseInDegree")
  private val multiNeighborsPreferenceParameter = NewParameter("multiNeighborsPreference", 0.0)
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
    Fingerprinting(
      (j \ "minimumOverlap").as[Int],
      (j \ "minimumSimilarity").as[Double],
      weightingModeParameter.fromJson(j),
      multiNeighborsPreferenceParameter.fromJson(j))

  val maxIterations = 30
}
case class Fingerprinting(
  minimumOverlap: Int,
  minimumSimilarity: Double,
  weightingMode: String = "InverseInDegree",
  multiNeighborsPreference: Double = 0.0)
    extends TypedMetaGraphOp[Fingerprinting.Input, Fingerprinting.Output] {
  import Fingerprinting._
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson =
    Json.obj("minimumOverlap" -> minimumOverlap, "minimumSimilarity" -> minimumSimilarity) ++
      weightingModeParameter.toJson(weightingMode) ++
      multiNeighborsPreferenceParameter.toJson(multiNeighborsPreference)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val leftPartitioner = inputs.left.rdd.partitioner.get
    val rightPartitioner = inputs.right.rdd.partitioner.get
    val candidatesPartitioner = inputs.candidates.rdd.partitioner.get
    val workingPartitioner = new HashPartitioner(
      inputs.rightEdges.rdd.partitions.size +
        inputs.leftEdges.rdd.partitions.size +
        inputs.candidates.rdd.partitions.size)

    // These are the two sides we are trying to connect.
    val leftCount = inputs.candidates.rdd
      .map { case (_, e) => e.src }
      .distinct
      .count

    val rightCount = inputs.candidates.rdd
      .map { case (_, e) => e.dst }
      .distinct
      .count

    // Returns an RDD that maps each source vertex of "es" to a list of their neighbors.
    // With each neighbor the edge weight and the neighbor's in-degree are also included.
    def outNeighbors(
      es: EdgeBundleRDD,
      weights: AttributeRDD[Double]): AttributeRDD[Iterable[(ID, (Double, Double))]] = {
      val esPartitioner = es.partitioner.get
      val weightedEdges = es.sortedJoin(weights)
      val inDegrees = weightedEdges
        .map { case (_, (e, w)) => e.dst -> w }
        .reduceBySortedKey(esPartitioner, _ + _)
      weightedEdges
        .map { case (_, (e, w)) => e.dst -> (w, e.src) }
        .sort(esPartitioner)
        .sortedJoin(inDegrees)
        .map { case (dst, ((w, src), dstInDegree)) => src -> (dst, (w, dstInDegree)) }
        .groupBySortedKey(workingPartitioner)
    }

    // Get the list of neighbors and their degrees for all candidates pairs.
    val candidates = inputs.candidates.rdd
      .map { case (_, e) => (e.dst, e.src) }
      .sort(workingPartitioner)
      .sortedLeftOuterJoin(outNeighbors(inputs.rightEdges.rdd, inputs.rightEdgeWeights.rdd))
      .map {
        case (rightId, (leftId, Some(rightNeighbors))) => (leftId, (rightId, rightNeighbors))
        case (rightId, (leftId, None)) => (leftId, (rightId, Seq()))
      }
      .sort(workingPartitioner)
      .sortedLeftOuterJoin(outNeighbors(inputs.leftEdges.rdd, inputs.leftEdgeWeights.rdd))
      .map {
        case (leftId, ((rightId, rightNeighbors), Some(leftNeighbors))) =>
          (leftId, leftNeighbors, rightId, rightNeighbors)
        case (leftId, ((rightId, rightNeighbors), None)) =>
          (leftId, Seq(), rightId, rightNeighbors)
      }

    // Calculate the similarity metric.
    val leftSimilarities =
      candidates.flatMap {
        case (leftId, leftNeighbors, rightId, rightNeighbors) =>
          val ln = leftNeighbors.toMap
          val rn = rightNeighbors.toMap
          val common = (ln.keySet intersect rn.keySet).toSeq
          if (common.isEmpty) {
            // Not just a shortcut. The formula would divide by zero if ln and rn are both empty.
            if (minimumOverlap > 0 || minimumSimilarity > 0) None
            else Some(leftId -> (rightId, 0.0))
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
            // This is somewhat debatable, but that's what Zubi's paper said: we take the average
            // of the indegree from left and from right if both are defined, otherwise just take the
            // one that's defined.
            val degrees = all.map(k => k -> (ld.get(k) ++ rd.get(k))).toMap
            val weights = weightingMode match {
              case "InDegree" => degrees.mapValues(ds => ds.sum / ds.size)
              case "InverseInDegree" => degrees.mapValues(ds => ds.size / ds.sum)
              case "InverseInDegreeBasedHybrid" => degrees.map {
                case (key, ds) =>
                  val inverseDeg = (ds.size / ds.sum) min 1
                  if (ln.contains(key)) key -> inverseDeg
                  else key -> (1 - inverseDeg)
              }
            }
            // Calculate similarity score.
            val isect = common.map(k => (lw(k) min rw(k)) * weights(k)).sum
            val union = all.map(k => (lw(k) max rw(k)) * weights(k)).sum
            val similarity = isect / (union + multiNeighborsPreference)
            if (similarity < minimumSimilarity) None
            else Some(leftId -> (rightId, similarity))
          }
      }.sort(candidatesPartitioner)
    val rightSimilarities =
      leftSimilarities.map { case (l, (r, s)) => (r, (l, s)) }.sort(candidatesPartitioner)

    // Run findStableMarriage with the smaller side as "ladies".
    def flipped(rdd: UniqueSortedRDD[ID, ID]): UniqueSortedRDD[ID, ID] = {
      rdd.map(_.swap).sortUnique(candidatesPartitioner)
    }
    val (leftToRight, rightToLeft) =
      if (rightCount < leftCount) {
        val ltr = findStableMarriage(rightSimilarities, leftSimilarities)
        (ltr, flipped(ltr))
      } else {
        val rtl = findStableMarriage(leftSimilarities, rightSimilarities)
        (flipped(rtl), rtl)
      }
    output(
      o.matching,
      leftToRight
        .map {
          case (src, dst) => Edge(src, dst)
        }
        // The 1:1 mapping is at most as large as the smaller side.
        .randomNumbered(if (rightCount < leftCount) rightPartitioner else leftPartitioner))
    output(o.leftSimilarities, leftSimilarities.sortedJoin(leftToRight)
      .flatMapValues {
        case ((simId, sim), id) if simId == id => Some(sim)
        case _ => None
      }.sortUnique(leftPartitioner))
    output(o.rightSimilarities, rightSimilarities.sortedJoin(rightToLeft)
      .flatMapValues {
        case ((simId, sim), id) if simId == id => Some(sim)
        case _ => None
      }.sortUnique(rightPartitioner))
  }

  // "ladies" is the smaller set. Returns a mapping from "gentlemen" to "ladies".
  def findStableMarriage(ladiesScores: SortedRDD[ID, (ID, Double)],
                         gentlemenScores: SortedRDD[ID, (ID, Double)]): UniqueSortedRDD[ID, ID] = {
    val ladiesPartitioner = ladiesScores.partitioner.get
    val gentlemenPartitioner = gentlemenScores.partitioner.get
    val gentlemenPreferences: UniqueSortedRDD[ID, Iterable[ID]] = gentlemenScores
      .groupByKey
      .mapValues(_.toSeq.sortBy(-_._2).map(_._1))
    val ladiesPreferences: UniqueSortedRDD[ID, Iterable[ID]] = ladiesScores
      .groupByKey
      .mapValues(_.toSeq.sortBy(-_._2).map(_._1))

    @annotation.tailrec
    def iterate(
      gentlemenCandidates: UniqueSortedRDD[ID, Iterable[ID]], iteration: Int): UniqueSortedRDD[ID, ID] = {

      val proposals = gentlemenCandidates.flatMap {
        case (gentleman, ladies) =>
          if (ladies.isEmpty) None else Some(ladies.head -> gentleman)
      }
      val proposalsByLadies = proposals.groupBySortedKey(ladiesPartitioner)
      val responsesByGentlemen = proposalsByLadies.sortedJoin(ladiesPreferences).map {
        case (lady, (proposals, preferences)) =>
          val ps = proposals.toSet
          // Preferences are symmetrical, so we will always find one here.
          preferences.find(g => ps.contains(g)).get -> lady
      }.sortUnique(gentlemenPartitioner)
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
    val outEdges = edges.map { case (_, e) => e.src -> e.dst }.sort(vertexPartitioner)

    // Returns the lines from the first attribute where the second attribute is undefined.
    def definedUndefined(defined: UniqueSortedRDD[ID, String],
                         undefined: UniqueSortedRDD[ID, String]): UniqueSortedRDD[ID, String] = {
      defined.sortedLeftOuterJoin(undefined).flatMapOptionalValues {
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

// Generates a list of candidate matches for Fingerprinting. A pair (x \in left, y \in right) will
// be considered a candidate if there is a z \in target such that (x, z) \in leftEdges and
// (y, z) \in rightEdges.
object FingerprintingCandidatesFromCommonNeighbors extends OpFromJson {
  class Input extends MagicInputSignature {
    val left = vertexSet
    val right = vertexSet
    val target = vertexSet
    val leftEdges = edgeBundle(left, target)
    val rightEdges = edgeBundle(right, target)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val candidates = edgeBundle(inputs.left.entity, inputs.right.entity)
  }
  def fromJson(j: JsValue) = FingerprintingCandidatesFromCommonNeighbors()
}
case class FingerprintingCandidatesFromCommonNeighbors()
    extends TypedMetaGraphOp[FingerprintingCandidatesFromCommonNeighbors.Input, FingerprintingCandidatesFromCommonNeighbors.Output] {
  import FingerprintingCandidatesFromCommonNeighbors._
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val neighborsPartitioner = new HashPartitioner(
      inputs.rightEdges.rdd.partitions.size + inputs.leftEdges.rdd.partitions.size)

    val leftByTargets =
      inputs.leftEdges.rdd.values.map(e => (e.dst, e.src)).sort(neighborsPartitioner)
    val rightByTargets =
      inputs.rightEdges.rdd.values.map(e => (e.dst, e.src)).sort(neighborsPartitioner)

    val candidatesByTargets = leftByTargets.sortedJoinWithDuplicates(rightByTargets)
    val resultPartitioner = rc.partitionerForNRows(candidatesByTargets.count)

    val distinctPairs =
      candidatesByTargets.values.distinct(resultPartitioner.numPartitions)
    output(
      o.candidates,
      distinctPairs
        .map { case (left, right) => Edge(left, right) }
        .randomNumbered(resultPartitioner))
  }
}
