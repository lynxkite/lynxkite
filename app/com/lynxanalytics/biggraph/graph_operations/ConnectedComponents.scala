package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes

// Generates a new vertex attribute, that is a component ID.
private object ConnectedComponents {
  // A "constant", but we want to use a small value in the unit tests.
  var maxEdgesProcessedLocally = 100000
}
case class ConnectedComponents(
    attribute: String) extends GraphOperation {
  type ComponentId = VertexId

  @transient lazy val outputSig = AttributeSignature
      .empty.addAttribute[Long](attribute).signature
  @transient lazy val outputCloner = AttributeSignature
      .empty.addAttribute[Long](attribute).cloner
  @transient lazy val outputIdx = outputSig.writeIndex[Long](attribute)

  def isSourceListValid(sources: Seq[BigGraph]): Boolean = sources.size == 1

  def execute(target: BigGraph,
              manager: GraphDataManager): GraphData = {
    val inputGraph = target.sources.head
    val inputData = manager.obtainData(inputGraph)
    val runtimeContext = manager.runtimeContext
    val sc = runtimeContext.sparkContext
    val cores = runtimeContext.numAvailableCores
    val partitioner = new spark.HashPartitioner(cores * 5)
    // Graph as edge lists. Empty lists are generated for degree-0 nodes.
    val edges = inputData.edges.map(e => (e.srcId, e.dstId))
    val graph = edges.groupByKey()
                     .mapValues(_.toSet)
                     .rightOuterJoin(inputData.vertices)
                     .mapValues(_._1.getOrElse(Set[VertexId]()))
    // Get VertexId -> ComponentId map.
    val components = getComponents(graph)
    // Put ComponentId in the new attribute.
    val vertices = inputData.vertices.join(components).mapValues({
      case (da, cid) =>
        outputCloner.clone(da).set(outputIdx, cid)
    })
    return new SimpleGraphData(target, vertices, inputData.edges)
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
    val invitations = graph.flatMap({ case (n, edges) =>
      if (Random.nextBoolean()) { // Host. All the neighbors are invited.
        (edges.map((_, n))
         ++ Seq((n, -1l)))  // -1 is note to self: stay at home, host party.
      } else {  // Guest. Can always stay at home at least.
        Seq((n, n))
      }
    })
    // Accept invitations.
    val parties = invitations.groupByKey().map({ case (n, invitations) =>
      if (invitations.size == 1 || invitations.contains(-1l)) {
        // Nowhere to go, or we are hosting a party. Stay at home.
        (n, n)
      } else {
        // Free to go. Go somewhere else.
        (n, invitations.find(_ != n).get)
      }
    })
    // Update edges. First, update the source (n->c) and flip the direction.
    val halfDone = graph.join(parties).flatMap({ case (n, (edges, c)) =>
      edges.map((_, c))
    }).groupByKey().mapValues(_.toSet)
    // Second, update the source, and merge the edge lists.
    val almostDone = halfDone.join(parties).map({ case (n, (edges, c)) =>
      (c, edges)
    }).groupByKey().map({ case (c, edges) => (c, edges.toSet.flatten - c) })
    // Third, remove finished components.
    val newGraph = almostDone.filter({ case (n, edges) => edges.nonEmpty })
    // Recursion.
    val newComponents: RDD[(VertexId, ComponentId)] = getComponents(newGraph)
    // We just have to map back the component IDs to the vertices.
    val guests = parties.map({ case (n, party) => (party, n) }).groupByKey()
    val components = guests.leftOuterJoin(newComponents).flatMap({
      case (party, (guests, component)) =>
        guests.map((_, component.getOrElse(party)))
    })
    return components
  }

  def getComponentsLocal(
    rdd: RDD[(VertexId, Set[VertexId])]): RDD[(VertexId, ComponentId)] = {
    import scala.collection.mutable
    val graph = rdd.collect.toMap
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
    return rdd.sparkContext.parallelize(components.toSeq)
  }

  def vertexAttributes(input: Seq[BigGraph]) = outputSig

  def edgeAttributes(input: Seq[BigGraph]) = input.head.edgeAttributes
}
