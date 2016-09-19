// Finds triangles in the graph and creates a segment for each of them.
// Currently it uses a naive but more or less optimized edge-iterator algorithm
// A possible way to further improve it:
// Park, Ha-Myung, Sung-Hyon Myaeng, and U. Kang. "PTE: Enumerating Trillion Triangles On Distributed Systems."
// http://www.kdd.org/kdd2016/papers/files/rfp0276-parkA.pdf
package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EnumerateTriangles extends OpFromJson {
  def fromJson(j: JsValue) = EnumerateTriangles(
    (j \ "needsBothDirections").as[Boolean])
}
case class EnumerateTriangles(needsBothDirections: Boolean = false)
    extends TypedMetaGraphOp[GraphInput, Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    new Segmentation(inputs.vs.entity)
  }

  override def toJson = Json.obj(
    "needsBothDirections" -> needsBothDirections)

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val outputPartitioner = inputs.es.rdd.partitioner.get

    // (1)
    // This removes all loop- and parallel edges, but keeps non-parallel multiple edges,
    // then labels remaining edges based on their orientation - see (2).
    // Also directs edges from smaller id to bigger id, making the graph acyclic - see (5).
    val filteredEdges = inputs.es.rdd
      .filter { case (_, Edge(src, dst)) => src != dst }
      .map { case (_, Edge(src, dst)) => (sortTuple(src, dst), if (src < dst) 1 else 2) }
      .sort(outputPartitioner)
      // As the last step it removes duplicates and computes the final labels.
      .reduceBySortedKey(outputPartitioner, _ | _)

    // (2)
    // Now we can apply the needsBothDirections constraint.
    // When lsb is set that means there was an edge directed from smaller id to bigger id.
    // When the bit above lsb is set that means there was a reversed one.
    // If needsBothDirections then only keep the edges where both bits are set.
    val simpleEdges =
      if (needsBothDirections)
        filteredEdges.filter(_._2 == 3).map(_._1)
      else
        filteredEdges.map(_._1)

    // (3)
    // The adjecencyArray will contain the list of neighbors
    // for each vertex according to simpleEdges.
    // This is necessary, because for this algorithm to work optimally
    // we need to access the neighbour sets of the source and the destination
    // of an edge in constant time.
    val adjacencyArray = simpleEdges
      .sort(outputPartitioner)
      .groupBySortedKey(outputPartitioner)
    // edgesWithNeigbors will be keyed by vertex pairs
    // and the values will be the neighbour lists of both vertices
    val edgesWithNeighbours = simpleEdges
      .sort(outputPartitioner)
      .sortedJoin(adjacencyArray)
      .map { case (src, (dst, nSrc)) => (dst, (src, nSrc)) }
      .sort(outputPartitioner)
      .sortedJoin(adjacencyArray)
      .map {
        case (dst, ((src, nSrc), nDst)) =>
          ((src, dst), (nSrc, nDst))
      }

    // (4)
    // Finally we can enumerate the triangles.
    val triangleList = edgesWithNeighbours.flatMap {
      case ((src, dst), (nSrc: Seq[ID], nDst: Seq[ID])) => {
        enumerateHeldTriangles(
          src,
          dst,
          nSrc,
          nDst)
      }
    }

    val indexedTriangleList = triangleList.randomNumbered(outputPartitioner)

    output(o.segments, indexedTriangleList.mapValues(_ => ()))
    output(o.belongsTo, indexedTriangleList.flatMap {
      case (identifier, triangle) => triangle.map(v => Edge(v, identifier))
    }.randomNumbered(outputPartitioner))
  }

  // (5)
  // This method checks if the source and the destination of an edge have common neighbours.
  // If so, then we found some triangles, and return all of them.
  // At this point the graph is guaranteed to be acyclic - see (1) -
  // so every triangle (as an induced subgraph) has exactly 1 vertex of indegree 2
  // which means the algorithm finds every triangle exactly once.
  def enumerateHeldTriangles(src: ID,
                             dst: ID,
                             nSrc: Seq[ID],
                             nDst: Seq[ID]) = {
    val triangleCollector = mutable.ArrayBuffer[List[ID]]()
    for (commonNeighbour <- nSrc.intersect(nDst)) {
      triangleCollector += List(src, dst, commonNeighbour)
    }
    triangleCollector
  }

  // (6)
  // This method directs the edge described as an (src, dst) tuple from smaller id to bigger id.
  def sortTuple(tuple: Tuple2[ID, ID]) = {
    if (tuple._1 > tuple._2) tuple.swap
    else tuple
  }
}
