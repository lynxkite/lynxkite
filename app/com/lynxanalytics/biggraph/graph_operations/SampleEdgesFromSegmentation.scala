// Given a segmentation, this operation creates an edge bundle. The edges
// will be a subset of co-occurring vertex pairs from each segment. Each
// vertex pair will have a fixed probability of getting to the subset.
// If two vertices co-occur multiple times, they still will be considered
// only once. Loop edges are also considered.
package com.lynxanalytics.biggraph.graph_operations

import breeze.stats.distributions.Poisson
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import org.apache.commons.math3.distribution.BinomialDistribution
import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SampleEdgesFromSegmentation extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val seg = vertexSet
    val belongsTo = edgeBundle(vs, seg)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               input: Input) extends MagicOutput(instance) {
    val es = edgeBundle(input.vs.entity, input.vs.entity)
    val multiplicity = edgeAttribute[Double](es)
  }
  def fromJson(j: JsValue) = SampleEdgesFromSegmentation(
    (j \ "prob").as[Double],
    (j \ "seed").as[Long])
}
import SampleEdgesFromSegmentation._
case class SampleEdgesFromSegmentation(prob: Double, seed: Long)
    extends TypedMetaGraphOp[Input, Output] {

  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "prob" -> prob,
    "seed" -> seed
  )

  // Takes a random sample of size numToGet from the pairs of values
  // from the vertices list.
  def sampleVertexPairs(
    vertices: Seq[Long],
    numToGet: Int,
    rng: JDKRandomGenerator): Seq[(ID, ID)] = {
    val n = vertices.size.toLong * vertices.size.toLong
    assert(numToGet <= n, s"sampleVertexPairs was requested to sample $numToGet from $n")
    val uniform = new UniformIntegerDistribution(rng, 0, vertices.size - 1);
    var set = mutable.Set[(ID, ID)]()
    while (set.size < numToGet) {
      val id1 = uniform.sample()
      val id2 = uniform.sample()
      val edge = (vertices(id1), vertices(id2))
      set += edge
    }
    set.toSeq
  }

  def getApproximateBinomialDistributionSample(
    n: Long,
    p: Double,
    rng: JDKRandomGenerator): Int = {
    val sampler =
      if (n < Int.MaxValue) {
        new BinomialDistribution(rng, n.toInt, p)
      } else {
        // Use poisson distribution to approximate binomial. According to my numeric
        // tests, if n > 50 and p < 0.1 the difference in the p of choosing an edge
        // are in the range of 1e-15. The reason we are branching with Int.MaxValue
        // and not 50 is that the implementation of poisson is much slower than the
        // implementation of binomial.
        val lambda = n * p
        val maxAllowedLambda = Int.MaxValue * 0.01
        if (lambda >= maxAllowedLambda) {
          // The problem is that the return value of poisson distribution is an int32.
          // Also, PoissonDistribution becomes really slow in those ranges.
          throw new AssertionError(
            s"There is a segment of size^2 = $n where the resulting sample size would be too large. " +
              s"Make sure that max(segmentSize^2) * probability < $maxAllowedLambda")
        }
        new PoissonDistribution(
          rng,
          lambda,
          PoissonDistribution.DEFAULT_EPSILON,
          PoissonDistribution.DEFAULT_MAX_ITERATIONS)
      }
    sampler.sample()
  }

  // Takes a sample from the pairs of values from the vertices list.
  // Each pair will have prob probability of being selected.
  def sampleVertexPairs(
    vertices: Iterable[Long],
    rng: JDKRandomGenerator): Seq[(ID, ID)] = {
    val membersSeq = vertices.toIndexedSeq
    val n = membersSeq.size.toLong * membersSeq.size.toLong
    val numSamples = getApproximateBinomialDistributionSample(n, prob, rng)
    sampleVertexPairs(membersSeq, numSamples, rng)
  }

  // Takes a sample from the set of edges that represent co-occurrences in
  // the segmentation. Each edge will have prob probability of being selected.
  def initialSampleEdges(
    segIdAndMembersArray: SortedRDD[ID, ArrayBuffer[ID]],
    partitioner: Partitioner,
    seed: Long): RDD[(ID, ID)] = {

    segIdAndMembersArray.mapPartitionsWithIndex {
      case (pidx, it) =>
        val pSeed = new Random((pidx << 16) + seed).nextLong()
        val rng = new JDKRandomGenerator()
        rng.setSeed(pSeed)
        it.flatMap {
          case (_, members) => {
            sampleVertexPairs(members, rng)
          }
        }
    }
  }

  // Extract the (vertex id -> segment id pairs) from the belongsTo RDD
  // and drop those pairs whose vertices are not present as endpoints
  // of preSelectedEdges.
  def getVertexToSegmentPairsForSampledEdges(
    belongsTo: UniqueSortedRDD[ID, Edge],
    preSelectedEdges: RDD[(ID, ID)],
    partitioner: Partitioner): SortedRDD[ID, ID] = {
    val idSet = preSelectedEdges
      .flatMap {
        case (src, dst) => Seq(src -> (), dst -> ())
      }
      .sort(belongsTo.partitioner.get)
      .distinctByKey()
    belongsTo.values.map(e => e.src -> e.dst)
      .sort(belongsTo.partitioner.get)
      .sortedJoin(idSet)
      .mapValues { case (dst, ()) => dst }
  }

  // For each vertex pair in selectedEdges, compute the number of segments in which
  // they co-occur. (This is the same as the number of parallel edges the operation
  // EdgesFromSegmentation would create.)
  def getEdgeMultiplicities(
    selectedEdges: RDD[(ID, ID)],
    belongsTo: UniqueSortedRDD[ID, Edge],
    partitioner: Partitioner): SortedRDD[(ID, ID), Int] = {
    val vsToSegs = getVertexToSegmentPairsForSampledEdges(belongsTo, selectedEdges, partitioner)
    val edgesToSrcSegList = selectedEdges
      .join(vsToSegs)
      .map { case (src, (dst, srcSeg)) => (src, dst) -> srcSeg }
      .groupBySortedKey(partitioner)
    val edgesToDstSegList = selectedEdges
      .map { case (src, dst) => (dst, src) }
      .join(vsToSegs)
      .map { case (dst, (src, dstSeg)) => (src, dst) -> dstSeg }
      .groupBySortedKey(partitioner)
    edgesToSrcSegList
      .sortedJoin(edgesToDstSegList)
      .mapValues {
        case (srcSegs, dstSegs) => {
          val intersectionSize = (srcSegs.toSet & dstSegs.toSet).size
          intersectionSize
        }
      }
  }

  // Re-samples the already selected edges to compensate that vertex pairs
  // with parallel edges between them had higher probability of being
  // selected in the previous step.
  def resampleEdgesToCompensateMultiplicities(
    preSelectedEdgesWithCounts: SortedRDD[(ID, ID), Int],
    partitioner: Partitioner,
    seed: Long): UniqueSortedRDD[ID, ((ID, ID), Int)] = {
    preSelectedEdgesWithCounts
      .mapPartitionsWithIndex {
        case (pidx, it) => {
          val pSeed = new Random((pidx << 16) + seed).nextLong()
          val rng = new JDKRandomGenerator()
          rng.setSeed(pSeed)
          it.flatMap {
            case (edge, count) => {
              val roll = rng.nextDouble()
              val limit = prob / (1.0 - Math.pow(1.0 - prob, count))
              if (roll < limit) {
                Some(edge, count)
              } else {
                None
              }
            }
          }
        }
      }.randomNumbered(partitioner)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val seedGenerator = new Random(seed)
    val belongsTo = inputs.belongsTo.rdd
    val segmentationPartitioner = belongsTo.partitioner.get
    val segToVs = belongsTo.values.map(e => e.dst -> e.src).sort(segmentationPartitioner)
    val segToMemberArray = segToVs.groupByKey
    val expectedNumberOfPreSelectedEdges =
      (segToMemberArray.values.map(edges => edges.size * edges.size).sum * prob).toLong
    val sampledEdgesPartitioner = rc.partitionerForNRows(expectedNumberOfPreSelectedEdges)

    val preSelectedEdges = initialSampleEdges(
      segToMemberArray, sampledEdgesPartitioner, seedGenerator.nextLong())
    val preSelectedEdgesWithCounts = getEdgeMultiplicities(
      preSelectedEdges, belongsTo, sampledEdgesPartitioner)
    val filteredEdges = resampleEdgesToCompensateMultiplicities(
      preSelectedEdgesWithCounts, sampledEdgesPartitioner, seedGenerator.nextLong())
    filteredEdges.cache()
    output(o.es, filteredEdges.mapValues { case ((src, dst), _) => Edge(src, dst) })
    output(o.multiplicity, filteredEdges.mapValues { case (_, count) => count.toDouble })
  }
}
