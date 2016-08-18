// Finds triangles in the graph.
// Currently it uses a naive but more or less optimized edge-iterator algorithm
// A possible way to further improve it:
// Park, Ha-Myung, Sung-Hyon Myaeng, and U. Kang. "PTE: Enumerating Trillion Triangles On Distributed Systems."
// http://www.kdd.org/kdd2016/papers/files/rfp0276-parkA.pdf
package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object FindTriangles extends OpFromJson {
  def fromJson(j: JsValue) = FindTriangles(
    (j \ "needsBothDirections").as[Boolean])
}
case class FindTriangles(needsBothDirections: Boolean = false)
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
    // remove loop- and parallel edges, keep non-parallel multiple edges
    // label edges based on their orientation, see (2)
    // direct edges from smaller to bigger id, this makes the graph acyclic, see (5)
    // then finally reduceByKey, which turned out to be faster than a 'distinct' here
    // and also made this clever labeling possible
    val filteredEdges = inputs.es.rdd
      .filter { case (_, Edge(src, dst)) => src != dst }
      .map { case (_, Edge(src, dst)) => (sortTuple(src, dst), if (src < dst) 1 else 2) }
      .sort(outputPartitioner)
      .reduceBySortedKey(outputPartitioner, _ | _)

    // (2)
    // now apply the needsBothDirections constraint
    // lsb is set means there was an edge directed from smaller to bigger id
    // bit above lsb is set means there was a reversed one
    // if needsBothDirection then only keep the ones where both bits are set
    // simpleEdges is the edge set of a simple graph, the edges we will actually work on
    val simpleEdges =
      if (needsBothDirections)
        filteredEdges.filter(_._2 == 3).map(_._1)
      else
        filteredEdges.map(_._1)

    // (3)
    // construct the adjacencyArray, and join it on the edge set
    // this is necessary, because for this algorithm to work optimally
    // we need to access the neighbour set of the source and destination
    // of an edge in constant time
    val adjacencyArray = simpleEdges
      .sort(outputPartitioner)
      .groupBySortedKey(outputPartitioner)
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
    // collect triangles
    val triangleList = edgesWithNeighbours.flatMap {
      case ((src, dst), (nSrc: Seq[ID], nDst: Seq[ID])) => {
        checkNeighbours(
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
  // check if the source and the destination of an edge have common neighbours
  // if so, then we found some triangles, return all of them
  // at this point the graph is guaranteed to be acyclic - see (1) -
  // so every triangle (as an induced subgraph) has exactly 1 vertex of indegree 2
  // which means this finds every triangle exactly once
  def checkNeighbours(src: ID,
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
  // direct the edge described as an (src, dst) tuple from smaller to bigger id
  def sortTuple(tuple: Tuple2[ID, ID]) = {
    if (tuple._1 > tuple._2) tuple.swap
    else tuple
  }
}
