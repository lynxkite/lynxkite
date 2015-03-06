package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object FindModularPartitioningV4 extends OpFromJson {
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
  def fromJson(j: JsValue) = FindModularPartitioningV4()

  case class ClusterData(
      // Edges fully within the cluster are counted twice in both touchingEdgeWeight and
      // insideEdgeWeight.
      touchingEdgeWeight: Double,
      insideEdgeWeight: Double,
      size: Int) {

    def add(connection: Double, other: ClusterData): ClusterData = {
      ClusterData(
        touchingEdgeWeight + other.touchingEdgeWeight,
        insideEdgeWeight + other.insideEdgeWeight + 2 * connection,
        size + other.size)
    }
    def remove(connection: Double, other: ClusterData): ClusterData = {
      ClusterData(
        touchingEdgeWeight - other.touchingEdgeWeight,
        insideEdgeWeight - other.insideEdgeWeight - 2 * connection,
        size - other.size)
    }
    def addNode(degree: Double, inClusterEdges: Double): ClusterData = {
      ClusterData(
        touchingEdgeWeight + degree,
        insideEdgeWeight + inClusterEdges,
        size + 1)
    }

    def value(totalDegreeSum: Double) =
      insideEdgeWeight / totalDegreeSum -
        touchingEdgeWeight * touchingEdgeWeight / totalDegreeSum / totalDegreeSum
  }

  def mergeValue(
    totalDegreeSum: Double,
    cluster1: ClusterData,
    cluster2: ClusterData,
    connection: Double): Double = {

    2 * connection / totalDegreeSum -
      2 * cluster1.touchingEdgeWeight * cluster2.touchingEdgeWeight /
      totalDegreeSum / totalDegreeSum
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
  def getMergeCandidates(
    totalDegreeSum: Double,
    clusters: scala.collection.Map[ID, ClusterData],
    cluster: ClusterData,
    members: Set[ID],
    edgeLists: Map[ID, Iterable[(ID, Double)]],
    containedIn: mutable.Map[ID, ID]): Map[ID, (Double, Double)] = {

    val clusterConnections = mutable.Map[ID, Double]().withDefaultValue(0.0)
    members.foreach {
      case vertex =>
        edgeLists(vertex)
          .filter { case (otherId, _) => !members.contains(otherId) }
          .foreach {
            case (otherId, weight) =>
              val otherClusterIdOpt = containedIn.get(otherId)
              otherClusterIdOpt.foreach(
                otherClusterId => clusterConnections(otherClusterId) += weight)
          }
    }
    clusterConnections
      .map {
        case (id, connection) =>
          (id, (mergeValue(totalDegreeSum, cluster, clusters(id), connection), connection))
      }
      .toMap
  }

  def refineClusters(
    iteration: Int,
    totalDegreeSum: Double,
    edgeLists: Map[ID, Iterable[(ID, Double)]],
    degrees: Map[ID, Double],
    containedIn: mutable.Map[ID, ID],
    start: spark.Accumulator[Double],
    end: spark.Accumulator[Double],
    increase: spark.Accumulator[Double]): Unit = {

    var localIncrease = 0.0
    val clusters = mutable.Map[ID, ClusterData]()
    val loops = edgeLists
      .map { case (id, edges) => id -> edges.find(_._1 == id).map(_._2).getOrElse(0.0) }
    edgeLists.foreach {
      case (id, edges) =>
        val clusterId = containedIn(id)
        // TODO: slightly hackish.
        clusters(clusterId) = clusters
          .getOrElse(clusterId, ClusterData(0, 0, 0))
          .addNode(
            degrees(id),
            edges
              .filter { case (otherId, weight) => containedIn.get(otherId) == Some(clusterId) }
              .map(_._2)
              .sum)
    }
    start += clusters.map(_._2.value(totalDegreeSum)).sum
    var changed = false
    var i = 0
    do {
      println("Iteration ", iteration, " a ", i)
      i += 1
      changed = false
      edgeLists.foreach {
        case (id, edges) =>
          val degree = degrees(id)
          val loop = loops(id)
          val homeClusterId = containedIn(id)
          val homeCluster = clusters(containedIn(id))
          if (homeCluster.size == 1) {
            val candidates = getMergeCandidates(
              totalDegreeSum,
              clusters,
              homeCluster,
              Set(id),
              edgeLists,
              containedIn)
            if (candidates.size > 0) {
              val (bestClusterId, (bestClusterValue, bestClusterConnection)) =
                candidates.maxBy { case (id, (value, connection)) => value }
              if (bestClusterValue > 0) {
                changed = true
                localIncrease += bestClusterValue
                containedIn(id) = bestClusterId
                clusters(bestClusterId) =
                  clusters(bestClusterId).add(bestClusterConnection, homeCluster)
                clusters.remove(homeClusterId)
              }
            }
          } else {
            val singletonCluster = ClusterData(degree, loop, 1)
            val homeConnection = edges
              .filter(_._1 != id)
              .filter { case (otherId, _) => containedIn.get(otherId) == Some(homeClusterId) }
              .map(_._2)
              .sum
            val homeClusterWithoutMe = homeCluster.remove(homeConnection, singletonCluster)
            clusters(homeClusterId) = homeClusterWithoutMe
            val candidates = getMergeCandidates(
              totalDegreeSum,
              clusters,
              singletonCluster,
              Set(id),
              edgeLists,
              containedIn)
            val finalCandidates = if (homeConnection == 0)
              // We don't have ANY connection to our own home. In this case candidates won't
              // contain the score of staying in our own cluster. Let's add that.
              candidates.updated(
                homeClusterId,
                (mergeValue(totalDegreeSum, singletonCluster, homeClusterWithoutMe, 0), 0.0))
            else candidates
            val (bestClusterId, (bestClusterValue, bestClusterConnection)) =
              finalCandidates.maxBy { case (id, (value, connection)) => value }
            val currentValue = finalCandidates(homeClusterId)._1
            if (bestClusterValue > currentValue) {
              // If we are strictly better then in the original cluster we move ...
              changed = true
              localIncrease += bestClusterValue - currentValue
              containedIn(id) = bestClusterId
              clusters(bestClusterId) =
                clusters(bestClusterId).add(bestClusterConnection, singletonCluster)
            } else {
              // otherwise we restore our home cluster.
              clusters(homeClusterId) = homeCluster
            }
          }
      }
    } while (changed)
    i = 0
    val contains = toContains(containedIn)
    do {
      changed = false
      println("Iteration ", iteration, " b ", i)
      i += 1
      val clusterIds = clusters.keys
      clusterIds.foreach { clusterId =>
        val cluster = clusters(clusterId)
        val members = contains(clusterId)
        val candidates = getMergeCandidates(
          totalDegreeSum,
          clusters,
          cluster,
          members,
          edgeLists,
          containedIn)
        if (candidates.size > 0) {
          val (bestClusterId, (bestClusterValue, bestClusterConnection)) =
            candidates.maxBy { case (id, (value, connection)) => value }
          if (bestClusterValue > 0) {
            changed = true
            localIncrease += bestClusterValue
            members.foreach { id =>
              containedIn(id) = bestClusterId
            }
            contains(bestClusterId) = contains(bestClusterId) ++ members
            clusters(bestClusterId) =
              clusters(bestClusterId).add(bestClusterConnection, cluster)
            clusters.remove(clusterId)
          }
        }
      }
    } while (changed)

    increase += localIncrease
    end += clusters.map(_._2.value(totalDegreeSum)).sum
  }
}

import FindModularPartitioningV4._
case class FindModularPartitioningV4() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vs = inputs.vs.rdd
    //val vPart = vs.partitioner.get
    val vPart = new spark.HashPartitioner(10)
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

    println(
      "Starting modularity: ",
      edgeLists.map {
        case (id, edges) =>
          val loop = edges.filter(_._1 == id).map(_._2).sum
          val degree = edges.map(_._2).sum
          loop / totalDegreeSum - degree * degree / totalDegreeSum / totalDegreeSum
      }.sum)

    val numParts = vPart.numPartitions
    println("Number of partitions: ", numParts)
    // TODO: I guess we should start from the vertex set instead.
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
                  val split = rnd.nextInt(numParts)
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
        val degrees = edgeLists.mapValues(edges => edges.map(_._2).sum)
        refineClusters(i, totalDegreeSum, edgeLists, degrees, containedIn, start, end, increase)
        containedIn.iterator
      }
      // TODO: We know all partitions are contained in the same split, so this couldbe optimized.
      members = newContainedIn.map { case (vx, part) => (part, vx) }.groupByKey()
      members.foreach(_ => ())
      println("Start", start.value)
      println("Increase", increase.value)
      println("End", end.value)
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
