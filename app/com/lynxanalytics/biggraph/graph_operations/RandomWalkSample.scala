package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import org.apache.spark.api.java.StorageLevels
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
  require(restartProbability < 1.0)
  require(restartProbability > 0.0)

  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  val maxTries = 10
  val maxStepsInOneTry = 1000000

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
    val sampler = new Sampler(nodes, edges)

    var actualSampleSize = 0L
    var tries = 0
    while (actualSampleSize < requestedSampleSize && tries < maxTries) {
      val (newSampleNodes, newSampleEdges) = {
        val startNode = randomNode(nodes, rnd.nextLong())
        val nodesMissing = requestedSampleSize - actualSampleSize
        val seed = rnd.nextInt()
        sampler.sample(nodesMissing, startNode, seed, maxStepsInOneTry)
      }
      nodesInSample = mergeSamples(nodesInSample, newSampleNodes)
      edgesInSample = mergeSamples(edgesInSample, newSampleEdges)
      actualSampleSize = nodesInSample.filter(_._2).count()
      tries += 1
    }
    output(o.verticesInSample, nodesInSample.mapValues(if (_) 1.0 else 0.0))
    output(o.edgesInSample, edgesInSample.mapValues(if (_) 1.0 else 0.0))
  }

  private def randomNode(nodes: VertexSetRDD, seed: Long) = nodes.takeSample(withReplacement = false, 1, seed).head._1

  private def mergeSamples(sample1: UniqueSortedRDD[ID, Boolean], sample2: UniqueSortedRDD[ID, Boolean]) = {
    sample1.sortedJoin(sample2).mapValues {
      case (isInPreviousSample, isInNewSample) => isInPreviousSample || isInNewSample
    }
  }

  class Sampler(nodes: VertexSetRDD, edges: EdgeBundleRDD) {
    val outEdgesPerNode = edges.map {
      case (edgeId, Edge(src, dest)) => src -> (dest, edgeId)
    }.groupByKey().map {
      case (id, it) => (id, it.toArray)
    }.sortUnique(nodes.partitioner.get)
    outEdgesPerNode.persist(StorageLevels.DISK_ONLY)
    val partitioner = outEdgesPerNode.partitioner.get
    type NodeId = ID
    type EdgeId = ID
    type WalkIdx = Long

    // samples at max requestedSampleSize unique nodes, less if it can't find enough
    def sample(requestedSampleSize: Long, startNodeID: ID, seed: Int, maxSteps: Int, batchSize: Int = 100)
              (implicit inputDatas: DataSet, rc: RuntimeContext) = {
      val rnd = new Random(seed)
      var multiWalk: RDD[(NodeId, (Walker, WalkIdx))] = {
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
      var walk: RDD[(NodeId, Option[EdgeId])] = rc.sparkContext.emptyRDD
      while (stepsMade < maxSteps && !hasEnoughUniqueNodes && !multiWalk.isEmpty()) {
        multiWalk = batchWalk(multiWalk, batchSize, rnd.nextInt())
        multiWalk.persist(StorageLevels.DISK_ONLY)
        val (deadPrefix, rest) = splitDeadPrefix(multiWalk)
        rest.persist(StorageLevels.DISK_ONLY)
        val firstLiveWalk = firstWalk(rest)
        walk = walk ++ flatToSingleWalk(deadPrefix) ++ flatToSingleWalk(firstLiveWalk)
        walk.persist(StorageLevels.DISK_ONLY)
        multiWalk = rest
        hasEnoughUniqueNodes = requestedSampleSize <= walk.map(_._1).distinct().count()
        stepsMade += batchSize
      }

      walk = tillNthUniqueNode(walk, n = requestedSampleSize)
      turnToSample(walk)
    }

    private def batchWalk(multiWalk: RDD[(NodeId, (Walker, WalkIdx))], batchSize: Int, seed: Int) = {
      val rnd = new Random(seed)
      Iterator.tabulate(batchSize) {
        _ => rnd.nextInt()
      }.foldLeft(multiWalk)(walkAStep(_, _, outEdgesPerNode, restartProbability))
    }

    private def walkAStep(multiWalk: RDD[(NodeId, (Walker, WalkIdx))],
                          seed: Int,
                          outEdgesPerNode: UniqueSortedRDD[NodeId, Array[(NodeId, EdgeId)]],
                          restartProbability: Double) = {
      multiWalk.sort(outEdgesPerNode.partitioner.get)
        .sortedJoin(outEdgesPerNode)
        .mapPartitionsWithIndex {
          case (pid, it) =>
            val rnd = new Random((pid << 16) + seed)
            it.map {
              case (currentNode, ((walker, walkerIdx), _)) if walker.dead => (currentNode, (walker, walkerIdx))
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

    private def splitDeadPrefix(multiWalk: RDD[(NodeId, (Walker, WalkIdx))]) = {
      val indicesOfLivingWalks = multiWalk.filter {
        case (_, (walker, _)) => !walker.dead
      }.map(walkIdx)
      val firstLiveIdx = if (!indicesOfLivingWalks.isEmpty()) {
        indicesOfLivingWalks.reduce(_ min _)
      } else {
        multiWalk.map(walkIdx).max() + 1
      }
      val deadPrefix = multiWalk.filter(walkIdx(_) < firstLiveIdx)
      val rest = multiWalk.filter(walkIdx(_) >= firstLiveIdx)
      (deadPrefix, rest)
    }

    private def firstWalk(multiWalk: RDD[(NodeId, (Walker, WalkIdx))]) = {
      if (!multiWalk.isEmpty()) {
        val minWalkIdx = multiWalk.map(walkIdx).min()
        multiWalk.filter(walkIdx(_) == minWalkIdx)
      } else {
        multiWalk
      }
    }

    private def walkIdx(walk: (NodeId, (Walker, WalkIdx))) = walk._2._2

    private def flatToSingleWalk(multiWalk: RDD[(NodeId, (Walker, WalkIdx))])(implicit rc: RuntimeContext) = {
      val walkersInOrder = {
        val partitioner = rc.partitionerForNRows(multiWalk.count())
        multiWalk.map(_._2).map(_.swap).sort(partitioner).map(_._2)
      }
      walkersInOrder.flatMap {
        case Walker(nodeIds, edgeIds, _) => nodeIds.zip(edgeIds).reverse
      }
    }

    private def tillNthUniqueNode(walk: RDD[(NodeId, Option[EdgeId])], n: Long) = {
      val indexedSteps = walk.zipWithIndex().map {
        case ((nodeId, optionalEdgeId), idx) => (nodeId, optionalEdgeId, idx)
      }
      indexedSteps.persist(StorageLevels.DISK_ONLY)

      val nodesByFirstOccurrence = indexedSteps.map {
        case (nodeId, _, idx) => (nodeId, idx)
      }.reduceByKey {
        case (idx1, idx2) => idx1 min idx2
      }.map {
        case (nodeId, firstIdxOfNode) => (firstIdxOfNode, nodeId)
      }.sortByKey()

      val lastStepToKeep = if (nodesByFirstOccurrence.count() < n) {
        // keep full walk
        indexedSteps.map(_._3).max()
      } else {
        // keep til the appearance of the n-th unique node
        nodesByFirstOccurrence.zipWithIndex().filter {
          case (_, idx) => idx < n
        }.keys.keys.max()
      }

      indexedSteps.filter {
        case (_, _, idx) => idx <= lastStepToKeep
      }.map {
        case (nodeId, edgeId, _) => (nodeId, edgeId)
      }
    }

    private def turnToSample(walk: RDD[(NodeId, Option[EdgeId])]) = {
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
