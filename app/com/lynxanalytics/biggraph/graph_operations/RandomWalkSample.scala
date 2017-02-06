package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD

import scala.util.{ Random, Try }

//object RandomWalkSample extends OpFromJson {
//  class Input extends MagicInputSignature {
//    val (vs, es) = graph
//  }
//  class Output(implicit instance: MetaGraphOperationInstance,
//               inputs: Input) extends MagicOutput(instance) {
//    val verticesInSample = vertexAttribute[Double](inputs.vs.entity)
//    val edgesInSample = edgeAttribute[Double](inputs.es.entity)
//  }
//  def fromJson(j: JsValue) = RandomWalkSample(
//    (j \ "requestedSampleSize").as[Long],
//    (j \ "restartProbability").as[Double],
//    (j \ "maxStartPoints").as[Int],
//    (j \ "seed").as[Int])
//}
//import RandomWalkSample._
//case class RandomWalkSample(requestedSampleSize: Long, restartProbability: Double,
//                            maxStartPoints: Int, seed: Int)
//    extends TypedMetaGraphOp[Input, Output] {
//  assert(restartProbability < 1.0,
//    "Restart probability at RandomWalkSample must be smaller than 1.0")
//
//  override val isHeavy = true
//  @transient override lazy val inputs = new Input()
//
//  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
//  override def toJson = Json.obj("requestedSampleSize" -> requestedSampleSize,
//    "restartProbability" -> restartProbability,
//    "maxStartPoints" -> maxStartPoints,
//    "seed" -> seed)
//
//  def execute(inputDatas: DataSet,
//              o: Output,
//              output: OutputBuilder,
//              rc: RuntimeContext): Unit = {
//    implicit val id = inputDatas
//    implicit val runtimeContext = rc
//    val nodes = inputs.vs.rdd
//    val edges = inputs.es.rdd
//
//    if (nodes.count() <= requestedSampleSize) {
//      output(o.verticesInSample, nodes.mapValues(_ => 1.0))
//      output(o.edgesInSample, edges.mapValues(_ => 1.0))
//      return
//    }
//
//    val rnd = new Random(seed)
//    var nodesInSample = nodes.mapValues(_ => false)
//    var edgesInSample = edges.mapValues(_ => false)
//    val walker = new Walker(nodes, edges, restartProbability)
//
//    var actualSampleSize = 0L
//    var numWalksPerformed = 0
//    while (actualSampleSize < requestedSampleSize && numWalksPerformed < maxStartPoints) {
//      val numNodesMissing = requestedSampleSize - actualSampleSize
//
//      // Simulate a random walk from a randomly selected start node
//      val (stepIdxWhenNodeFirstVisited, stepIdxWhenEdgeFirstTraversed) = {
//        val startNode = randomNode(nodes, rnd.nextLong())
//        // The run time of the sampling algorithm is proportional to the number of walk steps made
//        // without a restart (we break a long walk with restarts into multiple walks without a
//        // restart and handle these non-restarting walks in a parallel manner)
//        // To avoid long run times we cheat and don't wait very long for a restart to happen but
//        // force a restart after some steps. To set this artificial limit, we use two heuristics
//        // 1) 10 times the expected length of a normally non-restarting walk =
//        //    = 10 / restartProbability
//        // 2) 3 times the number of missing nodes = 3 * numNodesMissing
//        // where 10 and 3 are arbitrary numbers
//        val maxStepsWithoutRestart =
//          Try(10 / restartProbability).getOrElse(Double.MaxValue).toLong min (3 * numNodesMissing)
//        // 3 is an arbitrary number
//        val maxRestarts = (3 * numNodesMissing * restartProbability).toLong max 1L
//        walker.walk(startNode, maxStepsWithoutRestart, maxRestarts, rnd)
//      }
//      stepIdxWhenNodeFirstVisited.persist(StorageLevels.DISK_ONLY)
//
//      // Since the generated walk can visit more than numNodesMissing unique, not already sampled
//      // nodes, we compute an index i for which
//      // |{ node | node not already sampled; idx_node < i }| = numNodesMissing
//      // and add these nodes to the sample. Effectively, we add a sufficiently long prefix of the
//      // walk to the sample.
//      val stepIdxForNodesNotAlreadySampled = nodesInSample.
//        filter(!_._2).
//        sortedJoin(stepIdxWhenNodeFirstVisited).
//        mapValues(_._2)
//      val firstIdxToDrop = nthUniqueNodeLeft(stepIdxForNodesNotAlreadySampled, n = numNodesMissing)
//      val newSampleNodes = stepIdxWhenNodeFirstVisited.mapValues(_ < firstIdxToDrop)
//      val newSampleEdges = stepIdxWhenEdgeFirstTraversed.mapValues(_ < firstIdxToDrop)
//
//      // Add nodes and edges to the sample
//      nodesInSample = mergeSamples(nodesInSample, newSampleNodes)
//      nodesInSample.persist(StorageLevels.DISK_ONLY)
//      edgesInSample = mergeSamples(edgesInSample, newSampleEdges)
//      actualSampleSize = nodesInSample.filter(_._2).count()
//      numWalksPerformed += 1
//    }
//    output(o.verticesInSample, nodesInSample.mapValues(if (_) 1.0 else 0.0))
//    output(o.edgesInSample, edgesInSample.mapValues(if (_) 1.0 else 0.0))
//  }
//
//  private class Walker(nodes: VertexSetRDD, edges: EdgeBundleRDD, restartProbability: Double) {
//    private val outEdgesPerNode = edges.map {
//      case (edgeId, Edge(src, dest)) => src -> (dest, edgeId)
//    }.groupByKey().map {
//      case (id, it) => (id, it.toArray)
//    }.sortUnique(nodes.partitioner.get)
//    outEdgesPerNode.persist(StorageLevels.DISK_ONLY)
//
//    // Assigns an index to every node and edge. The exact index of a node has no meaning, it is
//    // only useful relative to the index of another node. If idx_node1 < idx_node2 then the first
//    // visit of node1 happened before the first visit of node2 during this walk. Later visits of
//    // the node are not captured by its index. Same logic applies for edge indices. Indices of
//    // never visited nodes/edges are set to Long.MaxValue
//    def walk(startNodeID: ID,
//             maxStepsWithoutRestart: Long,
//             maxRestarts: Long,
//             rnd: Random)(implicit rc: RuntimeContext):
//    (UniqueSortedRDD[ID, Long], UniqueSortedRDD[ID, Long]) = {
//      val numWalkers = maxRestarts
//      // We simulate multiple short, non-restarting walks instead of a single, long, restarting
//      // walk. Then we concatenate the short, non-restarting walks to form a long restarting walk.
//      // Since the part of a restarting walk that follows a restart is independent from the part
//      // before the restart, our simulation is equivalent of a simulation of the long, restarting
//      // walk.
//      // MultiWalkState keeps track of at which node the ith short, non-restarting walks is and a
//      // step index that increases monotonically as the walk advances. Moreover, if 'a' and 'b' are
//      // two short, non-restarting walks and 'a' is an earlier part of the final long, restarting
//      // walk than 'b', then every step index in walk 'a' is guaranteed to be smaller than any step
//      // index in walk 'b'.
//      var multiWalkState = {
//        // idxMultiplier = the first power of 10 which is bigger than maxStepsWithoutRestart
//        // => no short, non-restarting walk can be longer than idxMultiplier
//        val idxMultiplier = Math.pow(10, Math.log10(maxStepsWithoutRestart).toInt + 1).toLong
//        val range = rc.sparkContext.
//          parallelize(0L until numWalkers, nodes.partitioner.get.numPartitions)
//        range.map(idx => {
//          val stepIdx = idx * idxMultiplier
//          (startNodeID, stepIdx)
//        })
//      }
//      // stepIdxWhenNodeFirstVisited = n -> min { i | step with index i is at node n }
//      var stepIdxWhenNodeFirstVisited = {
//        val allUnvisited = nodes.mapValues(_ => Long.MaxValue)
//        minByKey(allUnvisited, multiWalkState)
//      }
//      // stepIdxWhenEdgeFirstTraversed = e -> min { i | step with index i traversed edge e }
//      var stepIdxWhenEdgeFirstTraversed = edges.mapValues(_ => Long.MaxValue)
//      var multiStepCnt = 0L
//      while (multiStepCnt < maxStepsWithoutRestart && !multiWalkState.isEmpty()) {
//        val (nodesVisitedNow, edgesTraversedNow) =
//          multiStep(multiWalkState, rnd, restartProbability)
//        nodesVisitedNow.persist(StorageLevels.DISK_ONLY)
//        stepIdxWhenNodeFirstVisited = minByKey(stepIdxWhenNodeFirstVisited, nodesVisitedNow)
//        stepIdxWhenEdgeFirstTraversed = minByKey(stepIdxWhenEdgeFirstTraversed, edgesTraversedNow)
//        multiWalkState = nodesVisitedNow
//        multiStepCnt += 1
//      }
//
//      (stepIdxWhenNodeFirstVisited, stepIdxWhenEdgeFirstTraversed)
//    }
//
//    private def minByKey(keyValue1: UniqueSortedRDD[ID, Long],
//                         keyValue2: RDD[(ID, Long)]): UniqueSortedRDD[ID, Long] = {
//      val x = keyValue2.reduceBySortedKey(keyValue1.partitioner.get, _ min _)
//      keyValue1.sortedLeftOuterJoin(x).mapValues {
//        case (oldIdx, newIdxOpt) => oldIdx min newIdxOpt.getOrElse(Long.MaxValue)
//      }
//    }
//
//    // computes the next step for every short, non-restarting walk
//    private def multiStep(multiWalkState: RDD[(ID, Long)],
//                          rnd: Random,
//                          restartProbability: Double): (RDD[(ID, Long)], RDD[(ID, Long)]) = {
//      val notRestartingWalks = {
//        val seed = rnd.nextInt()
//        multiWalkState.mapPartitionsWithIndex {
//          case (pid, it) =>
//            val rnd = new Random((pid << 16) + seed)
//            it.map(x => x -> rnd.nextDouble())
//        }.filter(_._2 > restartProbability).map(_._1)
//      }
//
//      val nextState = {
//        val seed = rnd.nextInt()
//        notRestartingWalks.sort(outEdgesPerNode.partitioner.get).sortedJoin(outEdgesPerNode)
//          .mapPartitionsWithIndex {
//            case (pid, it) =>
//              val rnd = new Random((pid << 16) + seed)
//              it.map {
//                case (_, (idx, edgesFromHere)) =>
//                  val rndIdx = rnd.nextInt(edgesFromHere.length)
//                  val (toNode, onEdge) = edgesFromHere(rndIdx)
//                  ((toNode, idx + 1), (onEdge, idx + 1))
//              }
//          }
//      }
//      nextState.persist(StorageLevels.DISK_ONLY)
//
//      (nextState.map(_._1), nextState.map(_._2))
//    }
//  }
//
//  private def randomNode(nodes: VertexSetRDD, seed: Long) =
//    nodes.takeSample(withReplacement = false, 1, seed).head._1
//
//  // returns argmax_i( |{ node | stepIdx when node first visited < i }| <= n )
//  private def nthUniqueNodeLeft(stepIdxWhenNodeFirstVisited: RDD[(ID, Long)], n: Long): Long = {
//    val nodesEverReached = stepIdxWhenNodeFirstVisited.filter(_._2 < Long.MaxValue)
//    nodesEverReached.persist(StorageLevels.DISK_ONLY)
//    if (nodesEverReached.count() > n) {
//      val nthNodeReached = nodesEverReached.
//        map(_.swap).
//        sortByKey().
//        zipWithIndex().
//        filter(_._2 < n).
//        map(_._1._1).
//        max()
//      nthNodeReached + 1
//    } else {
//      Long.MaxValue
//    }
//  }
//
//  private def mergeSamples(sample1: UniqueSortedRDD[ID, Boolean],
//                           sample2: UniqueSortedRDD[ID, Boolean]): UniqueSortedRDD[ID, Boolean] = {
//    sample1.sortedJoin(sample2).mapValues {
//      case (isInSample1, isInSample2) => isInSample1 || isInSample2
//    }
//  }
//}







// The sampling algorithm works as follows.
//
// 1) Visit some of the nodes and edges by the following logic
//  `numOfStartPoints` times
//      select a random start node
//      `numOfWalksFromOnePoint` times
//          perform a random walk on the graph from the selected node where the length of the walk
//          follows a geometric distribution with parameter `walkAbortionProbability`
//
// 2) Assign an index to every node and edge: the earlier the node was visited / edge was traversed
//    the smaller the index is. The index of nodes / edges never visited is higher than the index
//    of visited ones.
//
// 3) A sample can be obtained by filtering out nodes and edges of high indices. This index limit
//    has to be determined by the user.
//
// The actual implementation is different (since it has to be parallel) but the resulting indices
// are identical to the method described above.
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
    (j \ "numOfWalksFromOnePoint").as[Int],
    (j \ "walkAbortionProbability").as[Double],
    (j \ "numOfStartPoints").as[Int],
    (j \ "seed").as[Int])
}
import RandomWalkSample._
case class RandomWalkSample(numOfWalksFromOnePoint: Int, walkAbortionProbability: Double,
                            numOfStartPoints: Int, seed: Int)
    extends TypedMetaGraphOp[Input, Output] {
  assert(walkAbortionProbability < 1.0,
    "The probability of aborting a walk at RandomWalkSample must be smaller than 1.0")
  assert(walkAbortionProbability >= 0.01,
    "The probability of aborting a walk at RandomWalkSample must be greater or equal than 0.01")

  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  override def toJson = Json.obj(
    "numOfWalksFromOnePoint" -> numOfWalksFromOnePoint,
    "walkAbortionProbability" -> walkAbortionProbability,
    "numOfStartPoints" -> numOfStartPoints,
    "seed" -> seed)

  private type StepIdx = Long
  private type StepsRemaining = Long
  private type WalkState = (ID, (StepIdx, StepsRemaining))

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val nodes = inputs.vs.rdd
    val edges = inputs.es.rdd
    val rnd = new Random(seed)

    var multiWalkState: RDD[WalkState] = {
      // The run time of the sampling algorithm is proportional to the length of the longest walk.
      // To avoid long run times we cheat and don't wait very long for abortion but force it after
      // some steps. To set this artificial limit, we use the heuristics
      //    10 times the expected length of a walk = 10 / walkAbortionProbability
      // where 10 is an arbitrary number
      val maxStepsWithoutAbortion = (10 / walkAbortionProbability).toLong
      val walksToPerform = for {
        _ <- 0 until numOfStartPoints
        fromNode = randomNode(nodes, rnd.nextLong())
        _ <- 0 until numOfWalksFromOnePoint
        walkLength = geometric(rnd, p = walkAbortionProbability) min maxStepsWithoutAbortion
      } yield (fromNode, walkLength)
      val cumulativeWalkLength = walksToPerform.scanLeft(0L)(_ + _._2)
      val initialState = walksToPerform.zip(cumulativeWalkLength).map {
        // index of the first step in a walk = the sum of the length of all previous walks
        // remainingSteps = walkLength - 1, since we count the start node as well
        case ((node, walkLength), sumOfLengthOfPreviousWalks) =>
          (node, (sumOfLengthOfPreviousWalks, walkLength - 1))
      }
      rc.sparkContext.parallelize(initialState, nodes.partitioner.get.numPartitions)
    }
    multiWalkState.persist(StorageLevels.DISK_ONLY)

    var stepIdxWhenNodeFirstVisited = {
      val allUnvisited = nodes.mapValues(_ => Long.MaxValue)
      minByKey(allUnvisited, multiWalkState.map {case (node, (idx, _)) => (node, idx)} )
    }
    var stepIdxWhenEdgeFirstTraversed = edges.mapValues(_ => Long.MaxValue)
    val step = multiStepper(nodes, edges)

    while (!multiWalkState.isEmpty()) {
      val (nextState, edgesTraversed) = step(multiWalkState, rnd.nextInt())
      nextState.persist(StorageLevels.DISK_ONLY)
      stepIdxWhenNodeFirstVisited = minByKey(stepIdxWhenNodeFirstVisited,
        nextState.map {case (node, (idx, _)) => (node, idx)} )
      stepIdxWhenEdgeFirstTraversed = minByKey(stepIdxWhenEdgeFirstTraversed, edgesTraversed)
      multiWalkState = nextState
    }

    output(o.verticesInSample, stepIdxWhenNodeFirstVisited.mapValues(_.toDouble))
    output(o.edgesInSample, stepIdxWhenEdgeFirstTraversed.mapValues(_.toDouble))
  }

  def multiStepper(nodes: VertexSetRDD, edges: EdgeBundleRDD):
    (RDD[WalkState], Int) => (RDD[WalkState], RDD[(ID, Long)]) =
  {
    val outEdgesPerNode = edges.map {
      case (edgeId, Edge(src, dest)) => src -> (dest, edgeId)
    }.groupByKey().map {
      case (id, it) => (id, it.toArray)
    }.sortUnique(nodes.partitioner.get)
    outEdgesPerNode.persist(StorageLevels.DISK_ONLY)

    def continueWalk(walkState: WalkState): Boolean = walkState._2._2 > 0

    def step(multiWalkState: RDD[WalkState], seed: Int): (RDD[WalkState], RDD[(ID, Long)]) = {
      val nextState = multiWalkState.filter(continueWalk).sort(outEdgesPerNode.partitioner.get).
        sortedJoin(outEdgesPerNode).mapPartitionsWithIndex {
          case (pid, it) =>
            val rnd = new Random((pid << 16) + seed)
            it.map {
              case (_, ((idx, remainingSteps), edgesFromHere)) =>
                val rndIdx = rnd.nextInt(edgesFromHere.length)
                val (toNode, onEdge) = edgesFromHere(rndIdx)
                val stepIdx = idx + 1
                ((toNode, (stepIdx, remainingSteps - 1)), (onEdge, stepIdx))
            }
        }
      nextState.persist(StorageLevels.DISK_ONLY)

      (nextState.map(_._1), nextState.map(_._2))
    }

    step
  }

  private def randomNode(nodes: VertexSetRDD, seed: Long) =
    nodes.takeSample(withReplacement = false, 1, seed).head._1

  private def minByKey(keyValue1: UniqueSortedRDD[ID, Long],
                       keyValue2: RDD[(ID, Long)]): UniqueSortedRDD[ID, Long] = {
    val x = keyValue2.reduceBySortedKey(keyValue1.partitioner.get, _ min _)
    keyValue1.sortedLeftOuterJoin(x).mapValues {
      case (oldIdx, newIdxOpt) => oldIdx min newIdxOpt.getOrElse(Long.MaxValue)
    }
  }

  // Draws a random number from a geometric distribution with parameter p
  // http://math.stackexchange.com/questions/485448
  private def geometric(rnd: Random, p: Double): Int =
    (Math.log(rnd.nextDouble()) / Math.log(1 - p)).toInt + 1
}
