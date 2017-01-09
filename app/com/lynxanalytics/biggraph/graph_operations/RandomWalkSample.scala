package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD

import scala.util.{ Random, Try }

object RandomWalkSample extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val verticesInSample = vertexAttribute[Double](inputs.vs.entity)
    val edgesInSample = edgeAttribute[Double](inputs.es.entity)
  }
  def fromJson(j: JsValue) = RandomWalkSample(
    (j \ "requestedSampleSize").as[Long],
    (j \ "restartProbability").as[Double],
    (j \ "maxStartPoints").as[Int],
    (j \ "seed").as[Int])
}
import RandomWalkSample._
case class RandomWalkSample(requestedSampleSize: ID, restartProbability: Double, maxStartPoints: Int, seed: Int)
    extends TypedMetaGraphOp[Input, Output] {
  assert(restartProbability < 1.0, "Restart probability at RandomWalkSample must be smaller than 1.0")

  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("requestedSampleSize" -> requestedSampleSize,
    "restartProbability" -> restartProbability,
    "maxStartPoints" -> maxStartPoints,
    "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val nodes = inputs.vs.rdd
    val edges = inputs.es.rdd

    if (nodes.count() <= requestedSampleSize) {
      output(o.verticesInSample, nodes.mapValues(_ => 1.0))
      output(o.edgesInSample, edges.mapValues(_ => 1.0))
      return
    }

    val rnd = new Random(seed)
    var nodesInSample = nodes.mapValues(_ => false)
    var edgesInSample = edges.mapValues(_ => false)
    val walker = new Walker(nodes, edges, restartProbability)

    var actualSampleSize = 0L
    var tries = 0
    while (actualSampleSize < requestedSampleSize && tries < maxStartPoints) {
      val nodesMissing = requestedSampleSize - actualSampleSize
      val (nodeReachedIdx, edgeReachedIdx) = {
        val startNode = randomNode(nodes, rnd.nextLong())
        // The run time of the sampling algorithm is proportional to the number of walk steps made without a restart
        // (we break a long walk with restarts into multiple walks without a restart and handle these non-restarting
        // walks in a parallel manner)
        // To avoid long run times we cheat and don't wait very long for a restart to happen but force a restart after
        // some steps. To set this artificial limit, we use two heuristics
        // 1) 10 times the expected length of a normally non-restarting walk = 10 / restartProbability
        // 2) 3 times the number of missing nodes = 3 * nodesMissing
        // where 10 and 3 are arbitrary numbers
        val maxStepsWithoutRestart =
          Try(10 / restartProbability).getOrElse(Double.MaxValue).toLong min (3 * nodesMissing)
        // 3 is an arbitrary number
        val maxRestarts = (3 * nodesMissing * restartProbability).toLong max 1L
        walker.walk(startNode, maxStepsWithoutRestart, maxRestarts, rnd)
      }
      nodeReachedIdx.persist(StorageLevels.DISK_ONLY)
      val firstIdxToDrop = nthUniqueNodeLeft(nodeReachedIdx, n = nodesMissing)
      val newSampleNodes = nodeReachedIdx.mapValues(_ < firstIdxToDrop)
      val newSampleEdges = edgeReachedIdx.mapValues(_ < firstIdxToDrop)
      nodesInSample = mergeSamples(nodesInSample, newSampleNodes)
      nodesInSample.persist(StorageLevels.DISK_ONLY)
      edgesInSample = mergeSamples(edgesInSample, newSampleEdges)
      actualSampleSize = nodesInSample.filter(_._2).count()
      tries += 1
    }
    output(o.verticesInSample, nodesInSample.mapValues(if (_) 1.0 else 0.0))
    output(o.edgesInSample, edgesInSample.mapValues(if (_) 1.0 else 0.0))
  }

  private class Walker(nodes: VertexSetRDD, edges: EdgeBundleRDD, restartProbability: Double) {
    private val outEdgesPerNode = edges.map {
      case (edgeId, Edge(src, dest)) => src -> (dest, edgeId)
    }.groupByKey().map {
      case (id, it) => (id, it.toArray)
    }.sortUnique(nodes.partitioner.get)
    outEdgesPerNode.persist(StorageLevels.DISK_ONLY)

    // returns an idx associated with nodes and edges which increases monotonically with the index of step when the
    // node/edge was first reached
    // indices of unreached nodes/edges are Long.MaxValue
    def walk(startNodeID: ID,
             maxStepsWithoutRestart: Long,
             maxRestarts: Long,
             rnd: Random)(implicit rc: RuntimeContext): (UniqueSortedRDD[ID, Long], UniqueSortedRDD[ID, Long]) = {
      val numWalkers = maxRestarts
      // idxMultiplier = the first power of 10 which is bigger than maxStepsWithoutRestart
      // => no walker steps more than idxMultiplier without restart
      val idxMultiplier = Math.pow(10, Math.log10(maxStepsWithoutRestart).toInt + 1).toLong
      var multiWalkState = {
        val range = rc.sparkContext.parallelize(0L until numWalkers, nodes.partitioner.get.numPartitions)
        range.map {
          idx => (startNodeID, idx * idxMultiplier)
        }
      }
      var nodeFirstReachedAt = {
        val allUnreached = nodes.mapValues(_ => Long.MaxValue)
        updatedReachNumbers(allUnreached, multiWalkState)
      }
      var edgeFirstUsedAt = edges.mapValues(_ => Long.MaxValue)
      var multiStepCnt = 0L
      while (multiStepCnt < maxStepsWithoutRestart && !multiWalkState.isEmpty()) {
        val (nodesReachedNow, edgesReachedNow) = multiStep(multiWalkState, rnd, restartProbability)
        nodesReachedNow.persist(StorageLevels.DISK_ONLY)
        nodeFirstReachedAt = updatedReachNumbers(nodeFirstReachedAt, nodesReachedNow)
        edgeFirstUsedAt = updatedReachNumbers(edgeFirstUsedAt, edgesReachedNow)
        multiWalkState = nodesReachedNow
        multiStepCnt += 1
      }

      (nodeFirstReachedAt, edgeFirstUsedAt)
    }

    private def updatedReachNumbers(oldReachNumbers: UniqueSortedRDD[ID, Long],
                                    newReachNumbers: RDD[(ID, Long)]): UniqueSortedRDD[ID, Long] = {
      val x = newReachNumbers.reduceBySortedKey(oldReachNumbers.partitioner.get, _ min _)
      oldReachNumbers.sortedLeftOuterJoin(x).mapValues {
        case (idx, None) => idx
        case (oldIdx, Some(newIdx)) => oldIdx min newIdx
      }
    }

    private def multiStep(multiWalkState: RDD[(ID, Long)],
                          rnd: Random,
                          restartProbability: Double): (RDD[(ID, Long)], RDD[(ID, Long)]) = {
      val notRestartingWalks = {
        val seed = rnd.nextInt()
        multiWalkState.mapPartitionsWithIndex {
          case (pid, it) =>
            val rnd = new Random((pid << 16) + seed)
            it.map(x => x -> rnd.nextDouble())
        }.filter(_._2 > restartProbability).map(_._1)
      }

      val nextState = {
        val seed = rnd.nextInt()
        notRestartingWalks.sort(outEdgesPerNode.partitioner.get).sortedJoin(outEdgesPerNode).mapPartitionsWithIndex {
          case (pid, it) =>
            val rnd = new Random((pid << 16) + seed)
            it.map {
              case (_, (idx, edgesFromHere)) =>
                val rndIdx = rnd.nextInt(edgesFromHere.length)
                val (toNode, onEdge) = edgesFromHere(rndIdx)
                ((toNode, idx + 1), (onEdge, idx + 1))
            }
        }
      }
      nextState.persist(StorageLevels.DISK_ONLY)

      (nextState.map(_._1), nextState.map(_._2))
    }
  }

  private def randomNode(nodes: VertexSetRDD, seed: Long) = nodes.takeSample(withReplacement = false, 1, seed).head._1

  private def nthUniqueNodeLeft(nodeReachedIdx: RDD[(ID, Long)], n: Long): Long = {
    val nodesEverReached = nodeReachedIdx.filter(_._2 < Long.MaxValue)
    nodesEverReached.persist(StorageLevels.DISK_ONLY)
    if (nodesEverReached.count() > n) {
      val nthNodeReached = nodesEverReached.
        map(_.swap).
        sortByKey().
        zipWithIndex().
        filter(_._2 < n).
        map(_._1._1).
        max()
      nthNodeReached + 1
    } else {
      Long.MaxValue
    }
  }

  private def mergeSamples(sample1: UniqueSortedRDD[ID, Boolean],
                           sample2: UniqueSortedRDD[ID, Boolean]): UniqueSortedRDD[ID, Boolean] = {
    sample1.sortedJoin(sample2).mapValues {
      case (isInSample1, isInSample2) => isInSample1 || isInSample2
    }
  }
}
