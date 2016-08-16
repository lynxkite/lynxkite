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
    val outputPartitioner = rc.partitionerForNRows(inputs.es.rdd.count())

    // remove loop edges
    val filteredEdges = inputs.es.rdd.filter { case (_, Edge(src, dst)) => src != dst }
      .map { case (_, Edge(src, dst)) => (src, dst) }

    // now apply the needsBothDirections constraint
    val magicNumber = if (needsBothDirections) 2 else 0
    val simpleEdges =
      filteredEdges
        .map(tuple => (sortTuple(tuple), if (tuple._1 < tuple._2) 1 else 2))
        .sort(outputPartitioner)
        .reduceBySortedKey(outputPartitioner, _ | _)
        .filter { _._2 > magicNumber }
        .map { _._1 }
        .sort(outputPartitioner)

    // get all pairs of connected edges
    val semiTriangles = simpleEdges
      .sortedJoinWithDuplicates(simpleEdges)
      .filter { case (a, (b, c)) => c > b }
      .map(_.swap)
      .sort(outputPartitioner)

    // collect triangles
    val triangleList = semiTriangles
      .sortedJoinWithDuplicates(simpleEdges.map((_, ())).sort(outputPartitioner)) // x => (x, ())
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
