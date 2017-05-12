// Finds connected components in a graph.
// Uses the algorithm from "A Model of Computation for MapReduce" by Karloff et al.
// The edges in the input graph must all be symmetric.
package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.storage.StorageLevel

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

object ConnectedComponents extends OpFromJson {
  def fromJson(j: JsValue) = ConnectedComponents((j \ "maxEdgesProcessedLocally").as[Int])
}
case class ConnectedComponents(maxEdgesProcessedLocally: Int = 20000000)
    extends TypedMetaGraphOp[GraphInput, Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    new Segmentation(
      inputs.vs.entity,
      EdgeBundleProperties(
        isFunction = true,
        isEverywhereDefined = true,
        isReverseEverywhereDefined = true))
  }
  override def toJson = Json.obj("maxEdgesProcessedLocally" -> maxEdgesProcessedLocally)

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val inputEdges = inputs.es.rdd.values
      .map(edge => (edge.src, edge.dst))
    val inputVertices = inputs.vs.rdd
    val partitioner = inputVertices.partitioner.get
    val graph = inputEdges
      .groupBySortedKey(inputVertices.partitioner.get)
      .mapValues(_.toSet)
    // islands are not represented in the edge bundle as they have degree 0
    val ccEdges = inputVertices.sortedLeftOuterJoin(getComponents(graph, 0))
      .map {
        case (vId, (_, Some(cId))) => Edge(vId, cId)
        case (vId, (_, None)) => Edge(vId, vId)
      }
    output(o.belongsTo, ccEdges.randomNumbered(partitioner))
    val ccVertices = ccEdges.map(_.dst -> (()))
      .sort(partitioner)
      .distinctByKey
    output(o.segments, ccVertices)
  }

  type ComponentId = ID

  def getComponents(
    graph: SortedRDD[ID, Set[ID]], iteration: Int): UniqueSortedRDD[ID, ComponentId] = {
    // Need to take a count of edges, and then operate on the graph.
    // We best cache it here.
    graph.persist(StorageLevel.MEMORY_AND_DISK)
    if (graph.count == 0) {
      return graph.sparkContext.emptyRDD[(ID, ComponentId)].sortUnique(graph.partitioner.get)
    }
    val edgeCount = graph.map(_._2.size).reduce(_ + _)
    if (edgeCount <= maxEdgesProcessedLocally) {
      return getComponentsLocal(graph)
    } else {
      return getComponentsDist(graph, iteration)
    }
  }

  def getComponentsDist(graph: SortedRDD[ID, Set[ID]], iteration: Int): UniqueSortedRDD[ID, ComponentId] = {
    val partitioner = graph.partitioner.get

    // Each node decides if it is hosting a party or going out as a guest.
    val invitations = graph.mapPartitionsWithIndex {
      case (pidx, it) =>
        val seed = new Random((pidx << 16) + iteration).nextLong
        val rnd = new Random(seed)
        it.flatMap {
          case (n, edges) =>
            // nextBoolean is not random enough directly on the seed
            if (rnd.nextBoolean) {
              // Host. All the neighbors are invited.
              (edges.map((_, n)) +
                ((n, -1l))) // -1 is note to self: stay at home, host party.
            } else { // Guest. Can always stay at home at least.
              Seq((n, n))
            }
        }
    }

    // Accept invitations.
    val moves = invitations.groupBySortedKey(partitioner).mapValuesWithKeys {
      case (n, invIt) =>
        val invSeq = invIt.toSeq.sorted
        if (invSeq.size == 1 || invSeq.contains(-1l)) {
          // Nowhere to go, or we are hosting a party. Stay at home.
          n
        } else {
          // Free to go. Go somewhere else.
          invSeq.find(_ != n).get
        }
    }.persist(StorageLevel.MEMORY_AND_DISK)

    // Update edges. First, update the source (n->c) and flip the direction.
    val halfDone = graph.sortedJoin(moves).flatMap({
      case (n, (edges, c)) =>
        edges.map((_, c))
    }).groupBySortedKey(partitioner).mapValues(_.toSet)
    // Second, update the source, and merge the edge lists.
    val almostDone = halfDone.sortedJoin(moves).map {
      case (n, (edges, c)) =>
        (c, edges)
    }.groupBySortedKey(partitioner).mapValuesWithKeys {
      case (c, edgesList) =>
        edgesList.toSet.flatten - c
    }

    // Third, remove finished components.
    val newGraph = almostDone.filter({ case (n, edges) => edges.nonEmpty })
    // Recursion.
    val newComponents: UniqueSortedRDD[ID, ComponentId] = getComponents(newGraph, iteration + 1)
    // We just have to map back the component IDs to the vertices.
    val reverseMoves = moves.map(_.swap)
    val parties = reverseMoves.groupBySortedKey(partitioner)
    val components = parties.sortedLeftOuterJoin(newComponents).flatMap({
      case (party, (guests, component)) =>
        guests.map((_, component.getOrElse(party)))
    }).sortUnique(partitioner)
    components
  }

  def getComponentsLocal(
    graphRDD: SortedRDD[ID, Set[ID]]): UniqueSortedRDD[ID, ComponentId] = {
    // Moves all the data to one worker and processes it there.
    val p = graphRDD.coalesce(1)

    p.mapPartitions(
      { it =>
        val graph = it.toMap
        val components = mutable.Map[ID, ComponentId]()
        // Breadth-first search.
        for (node <- graph.keys) {
          if (!components.contains(node)) {
            components(node) = node
            val todo = mutable.Queue(node)
            while (todo.size > 0) {
              val v = todo.dequeue()
              for (u <- graph(v)) {
                if (!components.contains(u)) {
                  components(u) = node
                  todo.enqueue(u)
                }
              }
            }
          }
        }
        assert(components.size == graph.size, s"${components.size} != ${graph.size}")
        components.iterator
      }).sortUnique(graphRDD.partitioner.get)
  }
}
