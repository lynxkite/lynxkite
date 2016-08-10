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

    val inputPartitioner = inputs.vs.rdd.partitioner.get
    val numTasks = (rc.sparkContext.defaultParallelism * 5) max inputPartitioner.numPartitions
    val outputPartitioner = new HashPartitioner(numTasks)

    // remove loop- and parallel edges, keep non-parallel multiple edges
    val filteredEdges = inputs.es.rdd.filter { case (_, Edge(src, dst)) => src != dst }
      .map { case (_, Edge(src, dst)) => (src, dst) }.distinct()

    // now apply the needsBothDirections constraint
    // simpleEdges - simple graph, the edges we will actually work on
    // doubledEdges - simpleEdges plus its reversed, will be used to construct the adjacencyArray
    val (simpleEdges, doubledEdges) =
      if (needsBothDirections) {
        val dE = filteredEdges.intersection(filteredEdges.map { case (src, dst) => (dst, src) })
        val sE = dE.map { case (src, dst) => (src min dst, dst max src) }.distinct()
        (sE, dE)
      } else {
        val sE = filteredEdges.map { case (src, dst) => (src min dst, dst max src) }.distinct()
        val dE = sE.flatMap { case (src, dst) => List((src, dst), (dst, src)) }
        (sE, dE)
      }

    // construct the adjacencyArray, and join it on the edge set
    val adjacencyArray = doubledEdges.sort(outputPartitioner).groupBySortedKey(outputPartitioner)
    val edgesWithNeighbours = simpleEdges
      .sort(outputPartitioner).sortedJoin(adjacencyArray)
      .map { case (src, (dst, nSrc)) => (dst, (src, nSrc)) }
      .sort(outputPartitioner).sortedJoin(adjacencyArray)
      .map { case (dst, ((src, nSrc), nDst)) => ((src, dst), (nSrc, nDst)) }

    // collect triangles
    val triangleList = edgesWithNeighbours.flatMap {
      case ((src, dst), (nSrc: Seq[ID], nDst: Seq[ID])) => {
        val collector = mutable.ArrayBuffer[List[ID]]()
        CheckNeighbours(
          src,
          dst,
          nSrc.filter(v => v < src && v < dst),
          nDst.filter(v => v < src && v < dst),
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

  def CheckNeighbours(src: ID,
                      dst: ID,
                      nSrc: Seq[ID],
                      nDst: Seq[ID],
                      triangleCollector: mutable.ArrayBuffer[List[ID]]) = {
    for (commonNeighbour <- nSrc.intersect(nDst)) {
      triangleCollector += List(src, dst, commonNeighbour)
    }
  }
}
