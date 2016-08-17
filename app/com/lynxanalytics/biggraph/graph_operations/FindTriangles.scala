// Finds triangles in the graph.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.HashPartitioner

import scala.collection.mutable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object FindTriangles extends OpFromJson {
  def fromJson(j: JsValue) = FindTriangles(
    (j \ "needsBothDirections").as[Boolean])
}
case class FindTriangles(needsBothDirections: Boolean = false) extends TypedMetaGraphOp[GraphInput, Segmentation] {
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

    // remove loop- and parallel edges, keep non-parallel multiple edges
    val filteredEdges = inputs.es.rdd
      .filter { case (_, Edge(src, dst)) => src != dst }
      .map { case (_, Edge(src, dst)) => (sortTuple(src, dst), if (src < dst) 1 else 2) }
      .sort(outputPartitioner)
      .reduceBySortedKey(outputPartitioner, _ | _)

    // now apply the needsBothDirections constraint
    // simpleEdges - simple graph, the edges we will actually work on
    // doubledEdges - simpleEdges plus its reversed, will be used to construct the adjacencyArray
    val simpleEdges =
      if (needsBothDirections)
        filteredEdges.filter(_._2 == 3).map(_._1)
      else
        filteredEdges.map(_._1)
    val doubledEdges = simpleEdges.flatMap { case (src, dst) => List((src, dst), (dst, src)) }

    // construct the adjacencyArray, and join it on the edge set
    val adjacencyArray = doubledEdges
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
          ((src, dst), (nSrc.filter(v => v < src), nDst.filter(v => v < dst)))
      }

    // collect triangles
    val triangleList = edgesWithNeighbours.flatMap {
      case ((src, dst), (nSrc: Seq[ID], nDst: Seq[ID])) => {
        val collector = mutable.ArrayBuffer[List[ID]]()
        checkNeighbours(
          src,
          dst,
          nSrc.filter(v => v < dst),
          nDst.filter(v => v < src),
          collector)
        collector
      }
    }

    val indexedTriangleList = triangleList.randomNumbered(outputPartitioner)

    output(o.segments, indexedTriangleList.mapValues(_ => ()))
    output(o.belongsTo, indexedTriangleList.flatMap {
      case (identifier, triangle) => triangle.map(v => Edge(v, identifier))
    }.randomNumbered(outputPartitioner))
  }

  def checkNeighbours(src: ID,
                      dst: ID,
                      nSrc: Seq[ID],
                      nDst: Seq[ID],
                      triangleCollector: mutable.ArrayBuffer[List[ID]]) = {
    for (commonNeighbour <- nSrc.intersect(nDst)) {
      triangleCollector += List(src, dst, commonNeighbour)
    }
  }

  def sortTuple(tuple: Tuple2[ID, ID]) = {
    if (tuple._1 > tuple._2) tuple.swap
    else tuple
  }
}
