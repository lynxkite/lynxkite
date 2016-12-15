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
    (j \ "requestedSampleSize").as[Int],
    (j \ "seed").as[Int])
  def apply(restartProbability: Double, requestedSampleSize: Int, seed: Int) =
    new RandomWalkSample(restartProbability, requestedSampleSize, seed)
}
import com.lynxanalytics.biggraph.graph_operations.RandomWalkSample._
case class RandomWalkSample(restartProbability: Double,
                            requestedSampleSize: Int,
                            seed: Int)
  extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  @transient lazy val nodes = inputs.vs.rdd
  @transient lazy val edges = inputs.es.rdd

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson =
    Json.obj("restartProbability" -> restartProbability, "requestedSampleSize" -> requestedSampleSize, "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc

    val rnd = new Random(seed)
    var nodesInSample = nodes.mapValues(_ => false)
    var edgesInSample = edges.mapValues(_ => false)

    val outEdges = edges.map {
      case (edgeId, Edge(src, dest)) => src -> (dest, edgeId)
    }.groupByKey().map {
      case (id, it) => (id, it.toArray)
    }.sortUnique(edges.partitioner.get)

    val sampler = new Sampler(outEdges)

    var actualSampleSize = 0
    while (actualSampleSize < requestedSampleSize) {
      val (newSampleNodes, newSampleEdges) =
        sampler.sample(requestedSampleSize - actualSampleSize, randomNode(rnd.nextLong()), rnd.nextInt())
      nodesInSample = nodesInSample.sortedJoin(newSampleNodes).mapValues {
        case (a, b) => a || b
      }
      edgesInSample = edgesInSample.sortedJoin(newSampleEdges).mapValues {
        case (a, b) => a || b
      }
      actualSampleSize = nodesInSample.filter(_._2).count().toInt
    }
    output(o.verticesInSample, nodesInSample.mapValues(if (_) 1.0 else 0.0))
    output(o.edgesInSample, edgesInSample.mapValues(if (_) 1.0 else 0.0))
  }

  private def randomNode(seed: Long) = nodes.takeSample(withReplacement = false, 1, seed).head._1

  class Sampler(outEdges: UniqueSortedRDD[ID, Array[(ID, ID)]]) {
    // samples at max requestedSampleSize nodes, less if it can't find enough
    def sample(requestedSampleSize: Int, startNodeID: ID, seed: Int)
              (implicit inputDatas: DataSet, rc: RuntimeContext) = {
      val (initialState, stepsNeeded) = init(requestedSampleSize, startNodeID)
      val rnd = new Random(seed)
      val finalState = Iterator.tabulate(stepsNeeded) {
        _ => rnd.nextInt()
      }.foldLeft(initialState) { walkAStep }
      // we are cheating here: with 0.01 probability some walks haven't died yet but we consider them dead at this point
      turnToSample(finalState, requestedSampleSize)
    }

    private def init(requestedSampleSize: Int, startNodeID: ID)(implicit rc: RuntimeContext) = {
      // 3 is an arbitrary number
      val numWalkers = 3 * (requestedSampleSize * restartProbability).toInt
      val epsilon = 0.01
      // given n walkers, the probability that at least one of them haven't restarted after k steps is
      // 1 - (1 - (1 - restartProbability)^k)^n
      // the steps needed for the previous probability to be smaller than epsilon is
      // log_{1 - restartProbability}(1 - (1 - epsilon)^{1/n})
      // by applying the logarithmic identities it can be turned to the following form
      val stepsNeeded =
        Math.ceil(Math.log(1 - Math.pow(1 - epsilon, 1 / numWalkers)) / Math.log(1 - restartProbability)).toInt
      val initialState = {
        val partitioner = rc.partitionerForNRows(numWalkers)
        rc.sparkContext.parallelize(0L until numWalkers, partitioner.numPartitions).map(_ => WalkState(startNodeID))
      }
      (initialState, stepsNeeded)
    }

    private def walkAStep(state: RDD[WalkState], seed: Int) = state.zipWithIndex().map {
      case (s @ WalkState(nodeIds, _, _), walkIdx) => (nodeIds.head, (s, walkIdx))
    }.sort(state.partitioner.get).sortedJoin(outEdges).mapPartitionsWithIndex {
      case (pid, it) =>
        val rnd = new Random((pid << 16) + seed)
        it.map {
          case (_, ((s @ WalkState(_, _, true), walkIdx), _)) => (s, walkIdx)
          case (_, ((s, walkIdx), edgesFromHere)) =>
            if (rnd.nextDouble() < restartProbability) {
              (s.die, walkIdx)
            }
            else {
              val rndIdx = rnd.nextInt(edgesFromHere.length)
              ((s.walk _).tupled(edgesFromHere(rndIdx)), walkIdx)
            }
        }
    }.map {
      case (s, walkIdx) => (walkIdx, s)
    }.sort(state.partitioner.get).map(_._2)

    private def turnToSample(state: RDD[WalkState], requestedSampleSize: Int) = {
      val stepsWithIdx = state.flatMap {
        case WalkState(nodeIds, edgeIds, _) => nodeIds.zip(edgeIds).reverse
      }.zipWithIndex().map {
        case ((nodeId, edgeId), idx) => (nodeId, edgeId, idx)
      }

      // lastStepIdx = the index of the step when the requestedSampleSize-th unique node is visited
      val lastStepIdx = stepsWithIdx.map {
        case (nodeId, _, idx) => (nodeId, idx)
      }.reduceByKey {
        case (idx1, idx2) => idx1 min idx2
      }.map {
        case (nodeId, firstIdxOfNode) => (firstIdxOfNode, nodeId)
      }.sortByKey().zipWithIndex().filter {
        case (_, idx) => idx < requestedSampleSize
      }.keys.keys.max()

      val nodesAndEdgesInSample = stepsWithIdx.filter {
        case (_, _, idx) => idx < lastStepIdx
      }

      val nodesInSample = nodesAndEdgesInSample.map(_._1).distinct().map((_, true)).sortUnique(nodes.partitioner.get)
      val edgesInSample = nodesAndEdgesInSample.map(_._2).distinct().map((_, true)).sortUnique(edges.partitioner.get)
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
    private case class WalkState(nodeIds: List[ID], edgeIds:List[ID], died: Boolean) {
      def walk(dest: ID, edge: ID) = {
        require(!died)
        WalkState(dest :: nodeIds, edge :: edgeIds, died = false)
      }
      def die = WalkState(nodeIds, edgeIds, died = true)
    }
  }
}
