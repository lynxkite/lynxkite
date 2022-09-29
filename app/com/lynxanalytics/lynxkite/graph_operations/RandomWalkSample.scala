package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import scala.util.Random

// The sampling algorithm works as follows.
//
// 1) Visit some of the nodes and edges by the following logic
//  `numOfStartPoints` times
//      select a random start node
//      `numOfWalksFromOnePoint` times
//          perform a random walk on the graph from the selected node where the length of the walk
//          follows a geometric distribution with parameter `walkAbortionProbability`
//    NOTE: the order of the walks is important and will be used in the second point
//
// 2) Concatenate the walks based on their oder to form a single sequence of nodes / edges.
//
// 3) A sample can be obtained by taking a prefix of this sequence and de-duplicating the entries
//
// The actual implementation is different (since it has to be parallel) but the obtained samples
// are identical to the ones from the method described above.
object RandomWalkSample extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val vertexFirstVisited = vertexAttribute[Double](inputs.vs.entity)
    val edgeFirstTraversed = edgeAttribute[Double](inputs.es.entity)
  }
  def fromJson(j: JsValue) = RandomWalkSample(
    (j \ "numOfStartPoints").as[Int],
    (j \ "numOfWalksFromOnePoint").as[Int],
    (j \ "walkAbortionProbability").as[Double],
    (j \ "seed").as[Int])
}
import RandomWalkSample._
case class RandomWalkSample(
    numOfStartPoints: Int,
    numOfWalksFromOnePoint: Int,
    walkAbortionProbability: Double,
    seed: Int)
    extends SparkOperation[Input, Output] {
  assert(
    walkAbortionProbability < 1.0,
    "The probability of aborting a walk at RandomWalkSample must be smaller than 1.0")
  assert(
    walkAbortionProbability >= 0.01,
    "The probability of aborting a walk at RandomWalkSample can not be lower than 0.01")

  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  override def toJson = Json.obj(
    "numOfStartPoints" -> numOfStartPoints,
    "numOfWalksFromOnePoint" -> numOfWalksFromOnePoint,
    "walkAbortionProbability" -> walkAbortionProbability,
    "seed" -> seed)

  private type StepIdx = Double
  private object StepIdx {
    val MaxValue = Double.MaxValue
  }
  private type RemainingSteps = Int
  private type WalkState = (ID, (StepIdx, RemainingSteps))

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val nodes = inputs.vs.rdd
    val edges = inputs.es.rdd
    val rnd = new Random(seed)

    // init all `numOfStartPoints` * `numOfWalksFromOnePoint` walks to compute them in parallel
    // one walk is represented by a `WalkState` that describes 1) in which node the walk is
    // 2) a stepIdx that represents the position of this step in the final, concatenated sequence
    // and 3) the number of remaining steps to make before the walk is aborted
    var multiWalkState: RDD[WalkState] = {
      // The run time of the sampling algorithm is proportional to the length of the longest walk.
      // To avoid long run times we cheat and don't wait very long for abortion but force it after
      // some steps. To set this artificial limit, we use the heuristics
      //    2 times the expected length of a walk = 2 / walkAbortionProbability
      // where 2 is an arbitrary number
      val maxStepsWithoutAbortion = (2 / walkAbortionProbability).toInt
      val walksToPerform = for {
        fromNode <- randomNodes(nodes, n = numOfStartPoints, seed = rnd.nextLong())
        _ <- 0 until numOfWalksFromOnePoint
        walkLength = geometric(rnd, p = walkAbortionProbability) min maxStepsWithoutAbortion
      } yield (fromNode, walkLength)
      val cumulativeWalkLength = walksToPerform.scanLeft(0L)(_ + _._2)
      val initialState: Array[WalkState] = walksToPerform.zip(cumulativeWalkLength).map {
        // index of the first step of a walk = the sum of the length of all previous walks
        // remainingSteps = walkLength - 1, since we count the start node as well
        case ((node, walkLength), sumOfLengthOfPreviousWalks) =>
          (node, (sumOfLengthOfPreviousWalks.toDouble, walkLength - 1))
      }
      rc.sparkContext.parallelize(initialState, nodes.partitioner.get.numPartitions)
    }
    multiWalkState.persist(StorageLevel.DISK_ONLY)
    val maxRemainingSteps = multiWalkState.values.values.max()

    // we don't need the full sequence of steps for the sample, only the first visit of a node/edge
    // is interesting
    var stepIdxWhenNodeFirstVisited = {
      val allUnvisited = nodes.mapValues(_ => StepIdx.MaxValue)
      minByKey(allUnvisited, multiWalkState.map { case (node, (idx, _)) => (node, idx) })
    }
    // we have to use RDD instead of SortedRDD because localCheckpoint doesn't work on the later
    var stepIdxWhenEdgeFirstTraversed: RDD[(ID, StepIdx)] = edges.mapValues(_ => StepIdx.MaxValue)
    val step = MultiStepper(nodes, edges).step _

    for (i <- 1 to maxRemainingSteps) {
      val (nextState, edgesTraversed) = step(multiWalkState, rnd.nextInt())
      nextState.persist(StorageLevel.DISK_ONLY)

      stepIdxWhenNodeFirstVisited = minByKey(
        stepIdxWhenNodeFirstVisited,
        nextState.map { case (node, (idx, _)) => (node, idx) })
      stepIdxWhenEdgeFirstTraversed = minByKey(stepIdxWhenEdgeFirstTraversed, edgesTraversed)

      // Seems like Spark throws StackOverflowError when the lineage of an RDD grows too long.
      // `RDD#localCheckpoint` cuts the lineage of the RDD which reduces resilience but prevents
      // the errors.
      // On my machine, I got StackOverflowError after around 100 iterations without checkpointing
      // `stepIdxWhenNodeFirstVisited` and `stepIdxWhenEdgeFirstTraversed` and around 130 without
      // checkpointing `nexState`. (Note: without checkpointing `nextState` true divisors of 20
      // work as iteration number ;).
      if (i % 20 == 0) {
        nextState.localCheckpoint()

        stepIdxWhenNodeFirstVisited.persist(StorageLevel.DISK_ONLY)
        stepIdxWhenNodeFirstVisited.localCheckpoint()
        stepIdxWhenNodeFirstVisited.count()

        stepIdxWhenEdgeFirstTraversed.persist(StorageLevel.DISK_ONLY)
        stepIdxWhenEdgeFirstTraversed.localCheckpoint()
        stepIdxWhenEdgeFirstTraversed.count()
      }

      multiWalkState = nextState
    }

    val vs = stepIdxWhenNodeFirstVisited.sort(nodes.partitioner.get).asUniqueSortedRDD
    val es = stepIdxWhenEdgeFirstTraversed.sort(edges.partitioner.get).asUniqueSortedRDD
    output(o.vertexFirstVisited, vs.filter(_._2 < StepIdx.MaxValue))
    output(o.edgeFirstTraversed, es.filter(_._2 < StepIdx.MaxValue))
  }

  private case class MultiStepper(nodes: VertexSetRDD, edges: EdgeBundleRDD) {
    val outEdgesPerNode = edges.map {
      case (edgeId, Edge(src, dest)) => src -> (dest, edgeId)
    }.groupByKey().map {
      case (id, it) => (id, it.toArray)
    }.sortUnique(nodes.partitioner.get)
    outEdgesPerNode.persist(StorageLevel.DISK_ONLY)

    def step(multiWalkState: RDD[WalkState], seed: Int): (RDD[WalkState], RDD[(ID, StepIdx)]) = {
      val nextStateAndEdges = multiWalkState.filter(walkState => walkState._2._2 > 0).sort(
        outEdgesPerNode.partitioner.get).sortedJoin(outEdgesPerNode).mapPartitionsWithIndex {
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
      nextStateAndEdges.persist(StorageLevel.DISK_ONLY)
      val nexState = nextStateAndEdges.map(_._1)
      val edgesTraversed = nextStateAndEdges.map(_._2)

      (nexState, edgesTraversed)
    }
  }

  private def randomNodes(nodes: VertexSetRDD, n: Int, seed: Long) =
    nodes.takeSample(withReplacement = false, n, seed).map(_._1)

  private def minByKey(
      keyValue1: RDD[(ID, StepIdx)],
      keyValue2: RDD[(ID, StepIdx)]): RDD[(ID, StepIdx)] = {
    val x = keyValue2.reduceByKey(_ min _)
    keyValue1.leftOuterJoin(x).mapValues {
      case (oldIdx, newIdxOpt) => oldIdx min newIdxOpt.getOrElse(StepIdx.MaxValue)
    }
  }

  // Draws a random number from a geometric distribution with parameter p
  // http://math.stackexchange.com/questions/485448
  private def geometric(rnd: Random, p: Double): Int =
    (Math.log(rnd.nextDouble()) / Math.log(1 - p)).toInt + 1
}
