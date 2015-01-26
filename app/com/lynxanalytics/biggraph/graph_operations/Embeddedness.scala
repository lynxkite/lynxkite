package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.SortedSet
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object Embeddedness extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val embeddedness = edgeAttribute[Double](inputs.es.entity)
  }
  def fromJson(j: JsValue) = Embeddedness()
}
import Embeddedness._
case class Embeddedness() extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val nonLoopEdges = edges.filter { case (_, e) => e.src != e.dst }
    val vertices = inputs.vs.rdd
    val vertexPartitioner = vertices.partitioner.get
    val neighbors = ClusteringCoefficient.Neighbors(vertices, nonLoopEdges).all

    val bySrc = edges.map {
      case (eid, e) => e.src -> (eid, e)
    }.toSortedRDD(vertexPartitioner)
    val srcJoined = bySrc.sortedJoin(neighbors)
    val byDst = srcJoined.map {
      case (src, ((eid, e), srcNeighbors)) => e.dst -> (eid, srcNeighbors)
    }.toSortedRDD(vertexPartitioner)
    val dstJoined = byDst.sortedJoin(neighbors)

    val embeddedness = dstJoined.map {
      case (dst, ((eid, srcNeighbors), dstNeighbors)) =>
        eid -> ClusteringCoefficient.sortedIntersectionSize(srcNeighbors, dstNeighbors).toDouble
    }.toSortedRDD(edges.partitioner.get)
    output(o.embeddedness, embeddedness)
  }
}
