package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.{RDD, ShuffledRDD}

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
  require(restartProbability < 1.0)
  require(restartProbability > 0.0)

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
      new Sampler(nodes, edges, outEdgesPerNode)
    }

    var actualSampleSize = 0L
    var iterationCnt = 0
    while (actualSampleSize < requestedSampleSize && iterationCnt < maxIteration) {
      val (newSampleNodes, newSampleEdges) =
        sampler.sample(requestedSampleSize - actualSampleSize, randomNode(nodes, rnd.nextLong()), rnd.nextInt(), 1000000)
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

  class Sampler(nodes: VertexSetRDD, edges: EdgeBundleRDD, outEdgesPerNode: UniqueSortedRDD[ID, Array[(ID, ID)]]) {
    val partitioner = outEdgesPerNode.partitioner.get

    // samples at max requestedSampleSize unique nodes, less if it can't find enough
    def sample(requestedSampleSize: Long, startNodeID: ID, seed: Int, maxSteps: Int, batchSize: Int = 100)
              (implicit inputDatas: DataSet, rc: RuntimeContext) = {
      val rnd = new Random(seed)
      var multiWalk: RDD[(ID, (Walker, Long))] = {
        // 3 is an arbitrary number
        val numWalkers = 3 * (requestedSampleSize * restartProbability).toInt max 1
        val range = rc.sparkContext.parallelize(0L until numWalkers, partitioner.numPartitions)
        range.map {
          _ => (startNodeID, Walker(startNodeID))
        }.zipWithIndex().map {
          case ((head, walker), idx) => (head, (walker, idx))
        }.partitionBy(partitioner)
      }
      var stepsMade = 0
      var hasEnoughUniqueNodes = requestedSampleSize == 0L
      var walk: RDD[(ID, Option[ID])] = rc.sparkContext.emptyRDD
      while (stepsMade < maxSteps && !hasEnoughUniqueNodes) {
        multiWalk = batchWalk(multiWalk, batchSize, rnd.nextInt())
        val (deadPrefix, rest) = splitDeadPrefix(multiWalk)
        val firstLiveWalk = firstWalk(rest)
        walk = walk ++ flatToSingleWalk(deadPrefix) ++ flatToSingleWalk(firstLiveWalk)
        walk.persist(StorageLevels.DISK_ONLY)
        multiWalk = rest
        hasEnoughUniqueNodes = requestedSampleSize <= walk.map(_._1).distinct().count()
        stepsMade += batchSize
      }

      walk = tillNthUniqueNode(walk, requestedSampleSize)
      turnToSample(walk)
    }

    private def batchWalk(multiWalk: RDD[(ID, (Walker, Long))], batchSize: Int, seed: Int) = {
      val rnd = new Random(seed)
      Iterator.tabulate(batchSize) {
        _ => rnd.nextInt()
      }.foldLeft(multiWalk)(walkAStep(_, _, outEdgesPerNode, restartProbability))
    }

    private def walkAStep(multiWalk: RDD[(ID, (Walker, Long))],
                          seed: Int,
                          outEdgesPerNode: UniqueSortedRDD[ID, Array[(ID, ID)]],
                          restartProbability: Double) = {
      multiWalk.sort(outEdgesPerNode.partitioner.get)
        .sortedJoin(outEdgesPerNode)
        .mapPartitionsWithIndex {
          case (pid, it) =>
            val rnd = new Random((pid << 16) + seed)
            it.map {
              // walker is already dead
              case (currentNode, (walkerWithIdx @ (Walker(_, _, true), _), _)) => (currentNode, walkerWithIdx)
              // otherwise
              case (currentNode, ((walker, walkerIdx), edgesFromHere)) =>
                if (rnd.nextDouble() < restartProbability) {
                  (currentNode, (walker.die, walkerIdx))
                } else {
                  val rndIdx = rnd.nextInt(edgesFromHere.length)
                  val (toNode, onEdge) = edgesFromHere(rndIdx)
                  (toNode, (walker.walk(toNode, onEdge), walkerIdx))
                }
            }
        }
    }

    private def splitDeadPrefix(multiWalk: RDD[(ID, (Walker, Long))]) = {
      val indicesOfLivingWalks = multiWalk.filter {
        case (_, (walker, _)) => !walker.dead
      }.map(_._2._2)
      val maxIdx = multiWalk.map(_._2._2).max()
      val firstLiveIdx = if (!indicesOfLivingWalks.isEmpty()) indicesOfLivingWalks.reduce(_ min _) else maxIdx + 1

      val deadPrefix = multiWalk.filter(_._2._2 < firstLiveIdx)
      val rest = multiWalk.filter(_._2._2 >= firstLiveIdx)
      (deadPrefix, rest)
    }

    private def firstWalk(multiWalk: RDD[(ID, (Walker, Long))]) = {
      if (!multiWalk.isEmpty()) {
        val smallestIdx = multiWalk.map(_._2).map(_._2).min()
        multiWalk.filter(_._2._2 == smallestIdx)
      } else {
        multiWalk
      }
    }

    private def flatToSingleWalk(multiWalk: RDD[(ID, (Walker, Long))])(implicit rc: RuntimeContext) = {
      val walksInOrder = multiWalk.map(_._2).map(_.swap).sort(rc.partitionerForNRows(multiWalk.count())).map(_._2)
      walksInOrder.flatMap {
        case Walker(nodeIds, edgeIds, _) => nodeIds.zip(edgeIds).reverse
      }
    }

    private def tillNthUniqueNode(walk: RDD[(ID, Option[ID])], n: Long) = {
      val indexedSteps = walk.zipWithIndex().map {
        case ((nodeId, optionalEdgeId), idx) => (nodeId, optionalEdgeId, idx)
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
        case (_, _, idx) => idx <= lastStepIdx
      }.map {
        case (nodeId, edgeId, _) => (nodeId, edgeId)
      }
    }

    private def turnToSample(walk: RDD[(ID, Option[ID])]) = {
      val nodesInSample = walk.map(_._1).distinct().map((_, true)).sortUnique(nodes.partitioner.get)
      val edgesInSample =
        walk.map(_._2).filter(_.isDefined).map(_.get).distinct().map((_, true)).sortUnique(edges.partitioner.get)
      val allNodesMarked = nodes.sortedLeftOuterJoin(nodesInSample).mapValues {
        case (_, optional) => optional.getOrElse(false)
      }
      val allEdgesMarked = edges.sortedLeftOuterJoin(edgesInSample).mapValues {
        case (_, optional) => optional.getOrElse(false)
      }
      (allNodesMarked, allEdgesMarked)
    }
  }
}

object Walker {
  def apply(startNode: ID): Walker = new Walker(List(startNode), List(None), dead = false)
}
case class Walker(nodeIds: List[ID], edgeIds: List[Option[ID]], dead: Boolean) {
  def walk(toNode: ID, onEdge: ID) = {
    require(!dead)
    Walker(toNode :: nodeIds, Some(onEdge) :: edgeIds, dead = false)
  }
  def die = Walker(nodeIds, edgeIds, dead = true)
}
