package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object FindModularPartitioningV2 extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, edges) = graph
    val weights = edgeAttribute[Double](edges)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val partitions = vertexSet
    val belongsTo = edgeBundle(
      inputs.vs.entity, partitions, properties = EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) = FindModularPartitioningV2()

  class ClusterData(
      // Edges fully within the cluster are counted twice in both touchingEdgeWeight and
      // insideEdgeWeight.
      var touchingEdgeWeight: Double,
      var insideEdgeWeight: Double) {

    def addNode(connectionWeightToThis: Double, degree: Double): ClusterData = {
      touchingEdgeWeight += degree
      insideEdgeWeight += connectionWeightToThis
      this
    }

    def copy: ClusterData = new ClusterData(touchingEdgeWeight, insideEdgeWeight)
  }

  def additionValue(
    totalDegreeSum: Double,
    vertexDegree: Double,
    connectionToCluster: Double,
    cluster: ClusterData): Double = {

    connectionToCluster / totalDegreeSum -
      2 * cluster.touchingEdgeWeight * vertexDegree / totalDegreeSum / totalDegreeSum
  }
  def refineClusters(
    iteration: Int,
    totalDegreeSum: Double,
    edgeLists: Map[ID, Iterable[(ID, Double)]],
    degrees: Map[ID, Double],
    containedIn: mutable.Map[ID, ID]): Unit = {
    val clusters = mutable.Map[ID, ClusterData]()
    edgeLists.foreach {
      case (id, edges) =>
        val clusterId = containedIn(id)
        val clusterData = clusters.getOrElseUpdate(clusterId, new ClusterData(0, 0))
        clusterData.touchingEdgeWeight += degrees(id)
        clusterData.insideEdgeWeight += edges
          .filter { case (otherId, weight) => containedIn.get(otherId) == Some(clusterId) }
          .map(_._2)
          .sum
    }
    var changed = false
    var i = 0
    do {
      println("Iteration ", iteration, i)
      i += 1
      changed = false
      edgeLists.foreach {
        case (id, edges) =>
          val degree = degrees(id)
          val homeClusterId = containedIn(id)
          val homeCluster = clusters(containedIn(id))
          val clusterConnections = mutable.Map[ID, Double]().withDefaultValue(0.0)
          var loop = 0.0;
          edges.foreach {
            case (otherId, weight) =>
              // We do not count loops as connection to our current cluster.
              if (otherId == id) {
                loop = weight
              } else {
                val otherClusterIdOpt = containedIn.get(otherId)
                otherClusterIdOpt.foreach(
                  otherClusterId => clusterConnections(otherClusterId) += weight)
              }
          }
          val homeConnection = clusterConnections(homeClusterId)
          val homeClusterWithoutMe = homeCluster.copy
          homeClusterWithoutMe.touchingEdgeWeight -= degrees(id)
          homeClusterWithoutMe.insideEdgeWeight -= homeConnection + loop
          val exitHomeCost = additionValue(
            totalDegreeSum, degree, clusterConnections(homeClusterId), homeClusterWithoutMe)
          val foreignConnections = clusterConnections.filterKeys(_ != homeClusterId)
          if (foreignConnections.size > 0) {
            val (bestClusterId, bestClusterValue, bestClusterConnection) = foreignConnections
              .map {
                case (id, connection) =>
                  (id, additionValue(totalDegreeSum, degree, connection, clusters(id)), connection)
              }
              .maxBy(_._2)
            if (bestClusterValue > exitHomeCost) {
              //println(id + " exiting original partition " + homeClusterId + " for " + bestClusterId)
              //println("Exit home cost: " + exitHomeCost)
              //println("Target value: " + bestClusterValue)
              changed = true
              containedIn(id) = bestClusterId
              clusters(homeClusterId) = homeClusterWithoutMe
              clusters(bestClusterId).touchingEdgeWeight += degree
              clusters(bestClusterId).insideEdgeWeight += bestClusterConnection + loop
            }
          }
      }
    } while (changed)
  }
}

import FindModularPartitioningV2._
case class FindModularPartitioningV2() extends TypedMetaGraphOp[Input, Output] {
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
    val vPart = new spark.HashPartitioner(4)
    val edgeLists = inputs.edges.rdd.sortedJoin(inputs.weights.rdd)
      .map {
        case (id, (e, w)) =>
          if (e.src < e.dst) ((e.src, e.dst), w)
          else ((e.dst, e.src), w)
      }
      .reduceBySortedKey(vPart, _ + _)
      .flatMap { case ((v1, v2), w) => Iterator((v1, (v2, w)), (v2, (v1, w))) }
      .groupBySortedKey(vPart)

    /*
    val containedIn = edgeLists.mapPartitions { edgeListIt =>
      val edgeLists = edgeListIt.toMap
      val degrees = edgeLists.mapValues(edges => edges.map(_._2).sum)
      val totalDegreeSum = degrees.values.sum
      val res = mutable.Map(degrees.keys.map(id => (id, id)).toSeq: _*)
      refineClusters(totalDegreeSum, edgeLists, degrees, res)
      res.iterator
    }

    val members = containedIn.map { case (vx, part) => (part, vx) }*/
    val numParts = vPart.numPartitions
    println("Number of partitions: ", numParts)
    // TODO: I guess we should start from the vertex set instead.
    var members: RDD[(ID, Iterable[ID])] = edgeLists.mapValuesWithKeys(p => Seq(p._1))
    for (i <- (0 until 30)) {
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
        val totalDegreeSum = degrees.values.sum
        refineClusters(i, totalDegreeSum, edgeLists, degrees, containedIn)
        containedIn.iterator
      }
      // TODO: We know all partitions are contained in the same split, so this couldbe optimized.
      members = newContainedIn.map { case (vx, part) => (part, vx) }.groupByKey()
    }

    val partitions = members.randomNumbered(vPart).mapValues(_._2)
    output(o.partitions, partitions.mapValues(_ => ()))
    output(
      o.belongsTo,
      partitions
        .flatMap { case (p, members) => members.map(m => Edge(m, p)) }
        .randomNumbered(vPart))
  }
}
