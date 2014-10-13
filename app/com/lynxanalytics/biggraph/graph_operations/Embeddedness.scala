package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.SortedSet
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object Embeddedness {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val embeddedness = edgeAttribute[Double](inputs.es.entity)
  }
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

    val inNeighbors = nonLoopEdges
      .map { case (_, e) => e.dst -> e.src }
      .groupBySortedKey(vertexPartitioner)
      .mapValues(it => SortedSet(it.toSeq: _*).toArray)

    val outNeighbors = nonLoopEdges
      .map { case (_, e) => e.src -> e.dst }
      .groupBySortedKey(vertexPartitioner)
      .mapValues(it => SortedSet(it.toSeq: _*).toArray)

    val neighbors = vertices.sortedLeftOuterJoin(outNeighbors).sortedLeftOuterJoin(inNeighbors)
      .mapValues {
        case ((_, outs), ins) => sortedUnion(outs.getOrElse(Array()), ins.getOrElse(Array()))
      }

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
        eid -> sortedIntersectionSize(srcNeighbors, dstNeighbors).toDouble
    }.toSortedRDD(edges.partitioner.get)
    output(o.embeddedness, embeddedness)
  }

  private def sortedUnion(a: Array[ID], b: Array[ID]): Array[ID] = {
    val builder = new mutable.ArrayBuilder.ofLong
    var ai = 0
    var bi = 0
    while (ai < a.size && bi < b.size) {
      if (a(ai) == b(bi)) {
        builder += a(ai)
        ai += 1
        bi += 1
      } else if (a(ai) > b(bi)) {
        builder += b(bi)
        bi += 1
      } else {
        builder += a(ai)
        ai += 1
      }
    }
    for (i <- ai until a.size) builder += a(i)
    for (i <- bi until b.size) builder += b(i)
    builder.result()
  }

  private def sortedIntersectionSize(a: Array[ID], b: Array[ID]): Int = {
    var ai = 0
    var bi = 0
    var result = 0
    while (ai < a.size && bi < b.size) {
      if (a(ai) == b(bi)) {
        result += 1
        ai += 1
        bi += 1
      } else if (a(ai) > b(bi)) {
        bi += 1
      } else {
        ai += 1
      }
    }
    result
  }
}
