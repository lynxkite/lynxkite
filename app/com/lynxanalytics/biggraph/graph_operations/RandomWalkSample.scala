package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import org.apache.spark.rdd.RDD

import scala.util.Random

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
    (j \ "restartProbability").as[Double],
    (j \ "requestedSampleSize").as[Long],
    (j \ "seed").as[Int])
}
import com.lynxanalytics.biggraph.graph_operations.RandomWalkSample._
case class RandomWalkSample(restartProbability: Double,
                            requestedSampleSize: Long,
                            seed: Int)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  val maxIteration = 10

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson =
    Json.obj("restartProbability" -> restartProbability, "requestedSampleSize" -> requestedSampleSize, "seed" -> seed)

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

    val sampler = {
      val outEdgesPerNode = edges.map {
        case (edgeId, Edge(src, dest)) => src -> (dest, edgeId)
      }.groupByKey().map {
        case (id, it) => (id, it.toArray)
      }.sortUnique(edges.partitioner.get)
      new Sampler(outEdgesPerNode, nodes, edges)
    }

    var actualSampleSize = 0L
    var iterationCnt = 0
    while (actualSampleSize < requestedSampleSize && iterationCnt < maxIteration) {
      val (newSampleNodes, newSampleEdges) =
        sampler.sample(requestedSampleSize - actualSampleSize, randomNode(nodes, rnd.nextLong()), rnd.nextInt())
      nodesInSample = nodesInSample.sortedJoin(newSampleNodes).mapValues {
        case (isInPreviousSample, isInNewSample) => isInPreviousSample || isInNewSample
      }
      edgesInSample = edgesInSample.sortedJoin(newSampleEdges).mapValues {
        case (isInPreviousSample, isInNewSample) => isInPreviousSample || isInNewSample
      }
      actualSampleSize = nodesInSample.filter(_._2).count()
      iterationCnt += 1
    }
    output(o.verticesInSample, nodesInSample.mapValues(if (_) 1.0 else 0.0))
    output(o.edgesInSample, edgesInSample.mapValues(if (_) 1.0 else 0.0))
  }

  private def randomNode(nodes: VertexSetRDD, seed: Long) = nodes.takeSample(withReplacement = false, 1, seed).head._1

  class Sampler(outEdgesPerNode: UniqueSortedRDD[ID, Array[(ID, ID)]], nodes: VertexSetRDD, edges: EdgeBundleRDD) {

    // samples at max requestedSampleSize unique nodes, less if it can't find enough
    def sample(requestedSampleSize: Long, startNodeID: ID, seed: Int)(implicit inputDatas: DataSet, rc: RuntimeContext) = {
      // 3 is an arbitrary number
      val numWalkers = 3 * (requestedSampleSize * restartProbability).toInt

      val stepsNeeded = {
        val epsilon = 0.01
        // given n walkers, the probability that at least one of them haven't restarted after k steps is
        // 1 - (1 - (1 - restartProbability)^k)^n
        // the steps needed for the previous probability to be smaller than epsilon is
        // log_{1 - restartProbability}(1 - (1 - epsilon)^{1/n})
        // by applying the logarithmic identities it can be turned to the following form
        Math.ceil(Math.log(1 - Math.pow(1 - epsilon, 1 / numWalkers)) / Math.log(1 - restartProbability)).toInt
      }

      val initialState = {
        val partitioner = rc.partitionerForNRows(numWalkers)
        rc.sparkContext.parallelize(0L until numWalkers, partitioner.numPartitions).map(_ => WalkState(startNodeID))
      }.zipWithIndex()

      val finalState = {
        val rnd = new Random(seed)
        Iterator.tabulate(stepsNeeded) {
          _ => rnd.nextInt()
        }.foldLeft(initialState)(walkAStep).map {
          case (state, walkIdx) => (walkIdx, state)
        }.sort(initialState.partitioner.get).values
      }

      val walk = {
        // we are cheating: with epsilon probability some walks haven't died yet but we consider them dead at this point
        val fullWalk = foldToSingleWalk(finalState)
        tillNthUniqueNode(fullWalk, requestedSampleSize)
      }
      turnToSample(walk)
    }

    private def walkAStep(state: RDD[(WalkState, Long)], seed: Int) = state.map {
      case (s @ WalkState(currentNodeId :: _, _, _), walkIdx) => (currentNodeId, (s, walkIdx))
    }.sort(state.partitioner.get).sortedJoin(outEdgesPerNode).mapPartitionsWithIndex {
      case (pid, it) =>
        val rnd = new Random((pid << 16) + seed)
        it.map {
          case (_, ((s @ WalkState(_, _, true), walkIdx), _)) => (s, walkIdx)
          case (_, ((s, walkIdx), edgesFromHere)) =>
            if (rnd.nextDouble() < restartProbability) {
              (s.die, walkIdx)
            } else {
              val rndIdx = rnd.nextInt(edgesFromHere.length)
              val (toNode, onEdge) = edgesFromHere(rndIdx)
              (s.walk(toNode, onEdge), walkIdx)
            }
        }
    }

    private def foldToSingleWalk(state: RDD[WalkState]) = state.flatMap {
      case WalkState(nodeIds, edgeIds, _) => nodeIds.zip(edgeIds).reverse
    }

    private def tillNthUniqueNode(walk: RDD[(ID, ID)], n: Long) = {
      val indexedSteps = walk.zipWithIndex().map {
        case ((nodeId, edgeId), idx) => (nodeId, edgeId, idx)
      }

      // lastStepIdx = the index of the step when the requestedSampleSize-th unique node is visited
      val lastStepIdx = indexedSteps.map {
        case (nodeId, _, idx) => (nodeId, idx)
      }.reduceByKey {
        case (idx1, idx2) => idx1 min idx2
      }.map {
        case (nodeId, firstIdxOfNode) => (firstIdxOfNode, nodeId)
      }.sortByKey().zipWithIndex().filter {
        case (_, idx) => idx < n
      }.keys.keys.max()

      indexedSteps.filter {
        case (_, _, idx) => idx < lastStepIdx
      }.map {
        case (nodeId, edgeId, _) => (nodeId, edgeId)
      }
    }

    private def turnToSample(walk: RDD[(ID, ID)]) = {
      val nodesInSample = walk.map(_._1).distinct().map((_, true)).sortUnique(nodes.partitioner.get)
      val edgesInSample = walk.map(_._2).distinct().map((_, true)).sortUnique(edges.partitioner.get)
      val allNodesMarked = nodes.sortedLeftOuterJoin(nodesInSample).map {
        case (id, (_, optional)) => (id, optional.getOrElse(false))
      }
      val allEdgesMarked = edges.sortedLeftOuterJoin(edgesInSample).map {
        case (id, (_, optional)) => (id, optional.getOrElse(false))
      }
      (allNodesMarked.asUniqueSortedRDD, allEdgesMarked.asUniqueSortedRDD)
    }

    private object WalkState {
      def apply(startNode: ID): WalkState = new WalkState(List(startNode), Nil, died = false)
    }
    private case class WalkState(nodeIds: List[ID], edgeIds: List[ID], died: Boolean) {
      def walk(toNode: ID, onEdge: ID) = {
        require(!died)
        WalkState(toNode :: nodeIds, onEdge :: edgeIds, died = false)
      }
      def die = WalkState(nodeIds, edgeIds, died = true)
    }
  }
}
