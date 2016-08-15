// Finds triangles in the graph.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.HashPartitioner

import scala.collection.mutable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object FindTrianglesNew extends OpFromJson {
  def fromJson(j: JsValue) = FindTrianglesNew(
    (j \ "needsBothDirections").as[Boolean])
}
case class FindTrianglesNew(needsBothDirections: Boolean = false) extends TypedMetaGraphOp[GraphInput, Segmentation] {
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
      .map { case (_, Edge(src, dst)) => (src, dst) }

    // now apply the needsBothDirections constraint
    val simpleEdges =
      if (needsBothDirections)
        filteredEdges
          .distinct()
          .map(tuple => (sortTuple(tuple), 1))
          .sort(outputPartitioner)
          .reduceBySortedKey(outputPartitioner, _ + _)
          .filter { _._2 == 2 }
          .map { _._1 }
          .sort(outputPartitioner)
      else
        filteredEdges
          .map(sortTuple)
          .distinct()
          .sort(outputPartitioner)

     val degrees = simpleEdges
      .flatMap { case (src, dst) => List((src, 1), (dst, 1)) }
      .sort(outputPartitioner)
      .reduceBySortedKey(outputPartitioner, _ + _)
    val ultimateEdges = simpleEdges
      .sortedJoin(degrees)
      .map { case (src, (dst, srcDeg)) => (dst, (src, srcDeg)) }
      .sort(outputPartitioner)
      .sortedJoin(degrees)
      .map { case (dst, ((src, srcDeg), dstDeg)) => ((src, dst), (srcDeg, dstDeg)) }
    val mofoEdges = ultimateEdges.map { case ((src, dst), (srcDeg, dstDeg)) => sortTuple(srcDeg * 10000000L + src, dstDeg * 10000000L + dst) }.sort(outputPartitioner)

    // get all pairs of connected edges
    val semiTriangles = mofoEdges
      .sortedJoinWithDuplicates(mofoEdges)
      .filter { case (a, (b, c)) => c > b }
      .map(_.swap)
      .sort(outputPartitioner)

    // collect triangles
    val triangleList = semiTriangles
      .sortedJoinWithDuplicates(mofoEdges.map((_, ())).sort(outputPartitioner)) // x => (x, ())
      .map { case ((a, b), (c, ())) => List(a, b, c) }

    val indexedTriangleList = triangleList.randomNumbered(outputPartitioner)

    output(o.segments, indexedTriangleList.mapValues(_ => ()))
    output(o.belongsTo, indexedTriangleList.flatMap {
      case (identifier, triangle) => triangle.map(v => Edge(v, identifier))
    }.randomNumbered(outputPartitioner))
  }

  def sortTuple(tuple: Tuple2[ID, ID]) = {
    if (tuple._1 > tuple._2) tuple.swap
    else tuple
  }
}
