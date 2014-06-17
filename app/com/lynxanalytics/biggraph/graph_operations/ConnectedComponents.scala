package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.Random

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes
import com.lynxanalytics.biggraph.graph_api.attributes.SignatureExtension

// Generates a new vertex attribute, that is a component ID.
private object ConnectedComponents {
  // A "constant", but we want to use a small value in the unit tests.
  var maxEdgesProcessedLocally = 100000
}
@SerialVersionUID(6278532261938949359l) case class ConnectedComponents(
    outputAttribute: String) extends NewVertexAttributeOperation[VertexId] {
  @transient lazy val tt = typeTag[VertexId]

  type ComponentId = VertexId

  override def isSourceListValid(sources: Seq[BigGraph]): Boolean =
    super.isSourceListValid(sources) && sources.head.properties.symmetricEdges

  override def computeHolistically(inputData: GraphData,
                                   runtimeContext: RuntimeContext,
                                   vertexPartitioner: spark.Partitioner): RDD[(VertexId, ComponentId)] = {
    val sc = runtimeContext.sparkContext
    val cores = runtimeContext.numAvailableCores
    val inputEdges = inputData.edges.map(e => (e.srcId, e.dstId))
    // vertexPartitioner will be inherited by all derived RDDs, so joins
    // in getComponentsDist() are going to be fast.
    // Graph as edge lists. Does not include degree-0 vertices.
    val graph = inputEdges.groupByKey(vertexPartitioner).mapValues(_.toSet)
    // Get VertexId -> ComponentId map.
    return getComponents(graph, 0)
  }

  override def computeLocally(vid: VertexId, da: DenseAttributes): ComponentId = vid

  def getComponents(
    graph: RDD[(VertexId, Set[VertexId])], iteration: Int): RDD[(VertexId, ComponentId)] = {
    // Need to take a count of edges, and then operate on the graph.
    // We best cache it here.
    graph.persist(StorageLevel.MEMORY_AND_DISK)
    if (graph.count == 0) {
      return graph.sparkContext.emptyRDD[(VertexId, ComponentId)]
    }
    val edgeCount = graph.map(_._2.size).reduce(_ + _)
    if (edgeCount <= ConnectedComponents.maxEdgesProcessedLocally) {
      return getComponentsLocal(graph)
    } else {
      return getComponentsDist(graph, iteration)
    }
  }

  def getComponentsDist(
    graph: RDD[(VertexId, Set[VertexId])], iteration: Int): RDD[(VertexId, ComponentId)] = {

    val partitioner = graph.partitioner.get

    // Each node decides if it is hosting a party or going out as a guest.
    val invitations = graph.mapPartitionsWithIndex {
      case (pidx, it) =>
        val rnd = new Random((pidx << 16) + iteration)
        it.flatMap {
          case (n, edges) =>
            if (rnd.nextBoolean()) { // Host. All the neighbors are invited.
              (edges.map((_, n)) +
                ((n, -1l))) // -1 is note to self: stay at home, host party.
            } else { // Guest. Can always stay at home at least.
              Seq((n, n))
            }
        }
    }

    // Accept invitations.
    val moves = invitations.groupByKey(partitioner).mapPartitions(pit =>
      pit.map {
        case (n, invIt) =>
          val invSeq = invIt.toSeq.sorted
          if (invSeq.size == 1 || invSeq.contains(-1l)) {
            // Nowhere to go, or we are hosting a party. Stay at home.
            (n, n)
          } else {
            // Free to go. Go somewhere else.
            (n, invSeq.find(_ != n).get)
          }
      },
      preservesPartitioning = true
    ).persist(StorageLevel.MEMORY_AND_DISK)

    // Update edges. First, update the source (n->c) and flip the direction.
    val halfDone = graph.join(moves).flatMap({
      case (n, (edges, c)) =>
        edges.map((_, c))
    }).groupByKey(partitioner).mapValues(_.toSet)
    // Second, update the source, and merge the edge lists.
    val almostDone = halfDone.join(moves).map({
      case (n, (edges, c)) =>
        (c, edges)
    }).groupByKey(partitioner).mapPartitions(pit =>
      pit.map {
        case (c, edgesList) =>
          (c, edgesList.toSet.flatten - c)
      },
      preservesPartitioning = true
    )

    // Third, remove finished components.
    val newGraph = almostDone.filter({ case (n, edges) => edges.nonEmpty })
    // Recursion.
    val newComponents: RDD[(VertexId, ComponentId)] = getComponents(newGraph, iteration + 1)
    // We just have to map back the component IDs to the vertices.
    val reverseMoves = moves.map({ case (n, party) => (party, n) })
    val parties = reverseMoves.groupByKey(partitioner)
    val components = parties.leftOuterJoin(newComponents).flatMap({
      case (party, (guests, component)) =>
        guests.map((_, component.getOrElse(party)))
    }).partitionBy(partitioner)
    return components
  }

  def getComponentsLocal(
    graphRDD: RDD[(VertexId, Set[VertexId])]): RDD[(VertexId, ComponentId)] = {
    // Moves all the data to the dirver and processes it there.
    val p = graphRDD.collect

    val graph = p.toMap
    val components = mutable.Map[Long, Long]()
    var idx = 0
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

    return graphRDD.sparkContext.parallelize(components.toSeq).partitionBy(graphRDD.partitioner.get)
  }
}
