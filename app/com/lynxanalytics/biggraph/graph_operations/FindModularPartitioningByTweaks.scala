package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.util.Random

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

/* Operation to find a partitioning of a given graph with high modularity.

This implementation is based on iteratively doing small tweaks on the partitioning based on the view
visible in the individual data splits of data and then reshuffle.

This operation ignores the direction of edges.
*/
object FindModularPartitioningByTweaks extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, edges) = graph
    val weights = edgeAttribute[Double](edges)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val partitions = vertexSet
    val belongsTo = edgeBundle(
      inputs.vs.entity, partitions, properties = EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) = FindModularPartitioningByTweaks()

  case class PartitionData(
      // Edges fully within the partition are counted twice in both touchingEdgeWeight and
      // insideEdgeWeight.
      touchingEdgeWeight: Double,
      insideEdgeWeight: Double,
      size: Int) {

    def add(connection: Double, other: PartitionData): PartitionData = {
      PartitionData(
        touchingEdgeWeight + other.touchingEdgeWeight,
        insideEdgeWeight + other.insideEdgeWeight + 2 * connection,
        size + other.size)
    }
    def remove(connection: Double, other: PartitionData): PartitionData = {
      PartitionData(
        touchingEdgeWeight - other.touchingEdgeWeight,
        insideEdgeWeight - other.insideEdgeWeight - 2 * connection,
        size - other.size)
    }
    def addNode(degree: Double, inPartitionEdges: Double): PartitionData = {
      PartitionData(
        touchingEdgeWeight + degree,
        // We add inPartitionEdges only once here as it will be also added when processing the
        // other end of the edge.
        insideEdgeWeight + inPartitionEdges,
        size + 1)
    }

    def value(totalDegreeSum: Double) =
      insideEdgeWeight / totalDegreeSum -
        touchingEdgeWeight * touchingEdgeWeight / totalDegreeSum / totalDegreeSum
  }

  def mergeValue(
    totalDegreeSum: Double,
    partition1: PartitionData,
    partition2: PartitionData,
    connection: Double): Double = {

    val totalDegreeSumSquare = totalDegreeSum * totalDegreeSum
    2 * connection / totalDegreeSum -
      2 * partition1.touchingEdgeWeight * partition2.touchingEdgeWeight / totalDegreeSumSquare
  }
  def toContains(containedIn: scala.collection.Map[ID, ID]): mutable.Map[ID, Set[ID]] = {
    val res = mutable.Map[ID, Set[ID]]()
    val asSeq = containedIn.toSeq.map { case (vId, cId) => (cId, vId) }.sorted
    val vIds = asSeq.map(_._2)
    var lastCId: ID = 0
    var lastStartIdx: Int = -1
    for (idx <- 0 until asSeq.size) {
      val cId = asSeq(idx)._1
      if ((lastCId != cId) || lastStartIdx < 0) {
        if (lastStartIdx >= 0) {
          res(lastCId) = vIds.slice(lastStartIdx, idx).toSet
        }
        lastCId = cId
        lastStartIdx = idx
      }
    }
    if (lastStartIdx >= 0) {
      res(lastCId) = vIds.slice(lastStartIdx, asSeq.size).toSet
    }
    res
  }

  // Computes merge values for all partitions connected to a given partion.
  def getMergeCandidates(
    totalDegreeSum: Double,
    partitions: scala.collection.Map[ID, PartitionData],
    partition: PartitionData,
    members: Set[ID],
    edgeLists: Map[ID, Iterable[(ID, Double)]],
    containedIn: mutable.Map[ID, ID]): Map[ID, (Double, Double)] = {

    val partitionConnections = mutable.Map[ID, Double]().withDefaultValue(0.0)
    members.foreach {
      case vertex =>
        edgeLists(vertex)
          .filter { case (otherId, _) => !members.contains(otherId) }
          .foreach {
            case (otherId, weight) =>
              val otherPartitionIdOpt = containedIn.get(otherId)
              otherPartitionIdOpt.foreach(
                otherPartitionId => partitionConnections(otherPartitionId) += weight)
          }
    }
    partitionConnections
      .map {
        case (id, connection) =>
          (id, (mergeValue(totalDegreeSum, partition, partitions(id), connection), connection))
      }
      .toMap
  }

  def refinePartitions(
    iteration: Int,
    totalDegreeSum: Double,
    edgeLists: Map[ID, Iterable[(ID, Double)]],
    containedIn: mutable.Map[ID, ID],
    start: spark.Accumulator[Double],
    end: spark.Accumulator[Double],
    increase: spark.Accumulator[Double]): Unit = {

    var localIncrease = 0.0
    val partitions = mutable.Map[ID, PartitionData]()
    val degrees = edgeLists.mapValues(edges => edges.map(_._2).sum)
    val loops = edgeLists
      .map { case (id, edges) => id -> edges.find(_._1 == id).map(_._2).getOrElse(0.0) }
    edgeLists.foreach {
      case (id, edges) =>
        val partitionId = containedIn(id)
        partitions(partitionId) = partitions
          .getOrElse(partitionId, PartitionData(0, 0, 0))
          .addNode(
            degrees(id),
            edges
              .filter { case (otherId, weight) => containedIn.get(otherId) == Some(partitionId) }
              .map(_._2)
              .sum)
    }
    start += partitions.map(_._2.value(totalDegreeSum)).sum
    var changed = false
    var i = 0
    do {
      log.info(s"Doing modularity iteration $iteration tweaking nodes subiteration $i")
      i += 1
      changed = false
      edgeLists.foreach {
        case (id, edges) =>
          val degree = degrees(id)
          val loop = loops(id)
          val homePartitionId = containedIn(id)
          val homePartition = partitions(containedIn(id))
          if (homePartition.size == 1) {
            val candidates = getMergeCandidates(
              totalDegreeSum,
              partitions,
              homePartition,
              Set(id),
              edgeLists,
              containedIn)
            if (candidates.size > 0) {
              val (bestPartitionId, (bestPartitionValue, bestPartitionConnection)) =
                candidates.maxBy { case (id, (value, connection)) => value }
              if (bestPartitionValue > 0) {
                changed = true
                localIncrease += bestPartitionValue
                containedIn(id) = bestPartitionId
                partitions(bestPartitionId) =
                  partitions(bestPartitionId).add(bestPartitionConnection, homePartition)
                partitions.remove(homePartitionId)
              }
            }
          } else {
            val singletonPartition = PartitionData(degree, loop, 1)
            val homeConnection = edges
              .filter(_._1 != id)
              .filter { case (otherId, _) => containedIn.get(otherId) == Some(homePartitionId) }
              .map(_._2)
              .sum
            // We temporarily remove the vertex from his own partition.
            val homePartitionWithoutMe = homePartition.remove(homeConnection, singletonPartition)
            partitions(homePartitionId) = homePartitionWithoutMe
            val candidates = getMergeCandidates(
              totalDegreeSum,
              partitions,
              singletonPartition,
              Set(id),
              edgeLists,
              containedIn)
            val finalCandidates = if (homeConnection == 0)
              // We don't have ANY connection to our own home. In this case candidates won't
              // contain the score of staying in our own partition. Let's add that.
              candidates.updated(
                homePartitionId,
                (mergeValue(totalDegreeSum, singletonPartition, homePartitionWithoutMe, 0), 0.0))
            else candidates
            val (bestPartitionId, (bestPartitionValue, bestPartitionConnection)) =
              finalCandidates.maxBy { case (id, (value, connection)) => value }
            val currentValue = finalCandidates(homePartitionId)._1
            if (bestPartitionValue > currentValue) {
              // If we are strictly better then in the original partition we move ...
              changed = true
              localIncrease += bestPartitionValue - currentValue
              containedIn(id) = bestPartitionId
              partitions(bestPartitionId) =
                partitions(bestPartitionId).add(bestPartitionConnection, singletonPartition)
              // We've already removed the node from its original partition, so nothing else to do
              // here.
            } else {
              // otherwise we restore our home partition.
              partitions(homePartitionId) = homePartition
            }
          }
      }
    } while (changed)
    i = 0
    val contains = toContains(containedIn)
    do {
      log.info(s"Doing modularity iteration $iteration merging partitions subiteration $i")
      changed = false
      i += 1
      val partitionIds = partitions.keys
      partitionIds.foreach { partitionId =>
        val partition = partitions(partitionId)
        val members = contains(partitionId)
        val candidates = getMergeCandidates(
          totalDegreeSum,
          partitions,
          partition,
          members,
          edgeLists,
          containedIn)
        if (candidates.size > 0) {
          val (bestPartitionId, (bestPartitionValue, bestPartitionConnection)) =
            candidates.maxBy { case (id, (value, connection)) => value }
          if (bestPartitionValue > 0) {
            changed = true
            localIncrease += bestPartitionValue
            members.foreach { id =>
              containedIn(id) = bestPartitionId
            }
            contains(bestPartitionId) = contains(bestPartitionId) ++ members
            partitions(bestPartitionId) =
              partitions(bestPartitionId).add(bestPartitionConnection, partition)
            partitions.remove(partitionId)
          }
        }
      }
    } while (changed)

    increase += localIncrease
    end += partitions.map(_._2.value(totalDegreeSum)).sum
  }
}

import FindModularPartitioningByTweaks._
case class FindModularPartitioningByTweaks() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vs = inputs.vs.rdd
    val vPart = vs.partitioner.get
    val edgeLists = inputs.edges.rdd.sortedJoin(inputs.weights.rdd)
      .map {
        case (id, (e, w)) =>
          if (e.src < e.dst) ((e.src, e.dst), w)
          else ((e.dst, e.src), w)
      }
      .reduceBySortedKey(vPart, _ + _)
      .flatMap { case ((v1, v2), w) => Iterator((v1, (v2, w)), (v2, (v1, w))) }
      .groupBySortedKey(vPart)

    val totalDegreeSum = edgeLists.map { case (id, edges) => edges.map(_._2).sum }.sum

    val numSplits = vPart.numPartitions
    var members: RDD[(ID, Iterable[ID])] = edgeLists.mapValuesWithKeys(p => Seq(p._1))

    var i = 0
    var lastFive = List[Double]()
    do {
      val start = rc.sparkContext.accumulator(0.0)
      val end = rc.sparkContext.accumulator(0.0)
      val increase = rc.sparkContext.accumulator(0.0)
      val vertexMeta = members
        .mapPartitionsWithIndex(
          {
            case (idx, it) =>
              val seed = new Random((idx << 16) + i).nextLong
              val rnd = new Random(seed)
              it.flatMap {
                case (pid, vs) =>
                  val split = rnd.nextInt(numSplits)
                  vs.map(vid => vid -> (pid, split))
              }
          })
        .toSortedRDD(vPart)
      val splits = vertexMeta.sortedJoin(edgeLists)
        .map { case (vid, ((pid, split), edges)) => split -> (vid, pid, edges) }
        .toSortedRDD(vPart)

      val newContainedIn = splits.mapPartitions { vertexIt =>
        val asSeq = vertexIt.toSeq
        var containedIn = mutable.Map(
          asSeq.map { case (split, (vid, pid, edges)) => vid -> pid }: _*)
        val edgeLists = asSeq
          .map { case (split, (vid, pid, edges)) => vid -> edges }
          .toMap
        refinePartitions(i, totalDegreeSum, edgeLists, containedIn, start, end, increase)
        containedIn.iterator
      }
      // TODO: We know all partitions are contained in the same split, so this couldbe optimized.
      members = newContainedIn.map { case (vx, part) => (part, vx) }.groupByKey()
      members.foreach(_ => ())
      log.info(
        s"Modularity in iteration $i increased by ${increase.value} " +
          s"from ${start.value} to ${end.value}")
      lastFive = (increase.value +: lastFive).take(5)
      i += 1
    } while ((lastFive.size < 5) || (lastFive.sum > 0.005))

    val partitions = members.randomNumbered(vPart).mapValues(_._2)
    output(o.partitions, partitions.mapValues(_ => ()))
    output(
      o.belongsTo,
      partitions
        .flatMap { case (p, members) => members.map(m => Edge(m, p)) }
        .randomNumbered(vPart))
  }
}
