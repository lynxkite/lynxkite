package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import scala.util.Random

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes
import com.lynxanalytics.biggraph.graph_api.attributes.SignatureExtension

// Generates a new vertex attribute, that is a component ID.
private object ConnectedComponents {
  // A "constant", but we want to use a small value in the unit tests.
  var maxEdgesProcessedLocally = 100000
}
case class ConnectedComponents(
    outputAttribute: String) extends NewVertexAttributeOperation[VertexId] {
  @transient lazy val tt = typeTag[VertexId]

  type ComponentId = VertexId

  // TODO(darabos): Let it also check that the graph is undirected
  //                once we have properties on BigGraphs.
  override def isSourceListValid(sources: Seq[BigGraph]): Boolean =
    super.isSourceListValid(sources) && sources.head.properties.symmetricEdges

  def computeAttribute(inputData: GraphData,
                       runtimeContext: RuntimeContext): RDD[(VertexId, ComponentId)] = {
    val sc = runtimeContext.sparkContext
    val cores = runtimeContext.numAvailableCores
    // This partitioner will be inherited by all derived RDDs, so joins
    // in getComponentsDist() are going to be fast.
    val partitioner = new spark.HashPartitioner(cores * 5)
    // Graph as edge lists. Does not include degree-0 vertices.
    val inputEdges = inputData.edges.map(e => (e.srcId, e.dstId))
    val graph = inputEdges.groupByKey(partitioner).mapValues(_.toSet)
    // Get VertexId -> ComponentId map.
    return getComponents(graph)
  }

  override def defaultValue(vid: VertexId, da: DenseAttributes): ComponentId = {
    vid
  }

  def getComponents(
    graph: RDD[(VertexId, Set[VertexId])]): RDD[(VertexId, ComponentId)] = {
    // Need to take a count of edges, and then operate on the graph.
    // We best cache it here.
    graph.cache()
    if (graph.count == 0) {
      return new spark.rdd.EmptyRDD[(VertexId, ComponentId)](graph.sparkContext)
    }
    val edgeCount = graph.map(_._2.size).reduce(_ + _)
    if (edgeCount <= ConnectedComponents.maxEdgesProcessedLocally) {
      return getComponentsLocal(graph)
    } else {
      return getComponentsDist(graph)
    }
  }

  def getComponentsDist(
    graph: RDD[(VertexId, Set[VertexId])]): RDD[(VertexId, ComponentId)] = {
    // Each node decides if it is hosting a party or going out as a guest.
    val invitations = graph.flatMap({
      case (n, edges) =>
        if (Random.nextBoolean()) { // Host. All the neighbors are invited.
          (edges.map((_, n)) +
            ((n, -1l))) // -1 is note to self: stay at home, host party.
        } else { // Guest. Can always stay at home at least.
          Seq((n, n))
        }
    })
    // Accept invitations.
    val moves = invitations.groupByKey().map({
      case (n, invitations) =>
        if (invitations.size == 1 || invitations.contains(-1l)) {
          // Nowhere to go, or we are hosting a party. Stay at home.
          (n, n)
        } else {
          // Free to go. Go somewhere else.
          (n, invitations.find(_ != n).get)
        }
    })
    // Update edges. First, update the source (n->c) and flip the direction.
    val halfDone = graph.join(moves).flatMap({
      case (n, (edges, c)) =>
        edges.map((_, c))
    }).groupByKey().mapValues(_.toSet)
    // Second, update the source, and merge the edge lists.
    val almostDone = halfDone.join(moves).map({
      case (n, (edges, c)) =>
        (c, edges)
    }).groupByKey().map({
      case (c, edgesList) =>
        (c, edgesList.toSet.flatten - c)
    })
    // Third, remove finished components.
    val newGraph = almostDone.filter({ case (n, edges) => edges.nonEmpty })
    // Recursion.
    val newComponents: RDD[(VertexId, ComponentId)] = getComponents(newGraph)
    // We just have to map back the component IDs to the vertices.
    val reverseMoves = moves.map({ case (n, party) => (party, n) })
    val parties = reverseMoves.groupByKey()
    val components = parties.leftOuterJoin(newComponents).flatMap({
      case (party, (guests, component)) =>
        guests.map((_, component.getOrElse(party)))
    })
    return components
  }

  def getComponentsLocal(
    graphRDD: RDD[(VertexId, Set[VertexId])]): RDD[(VertexId, ComponentId)] = {
    // Moves all the data to a single worker and processes it there.
    val componentsRDD = graphRDD.coalesce(1).mapPartitions(p => {
      import scala.collection.mutable
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
      components.iterator
    })
    // Split up the result by the same partitioner as was used in the input.
    return componentsRDD.partitionBy(graphRDD.partitioner.get)
  }
}
