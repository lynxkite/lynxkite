package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object ForceLayout3D {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val positions = vertexAttribute[(Double, Double, Double)](inputs.vs.entity)
  }
}
import ForceLayout3D._
case class ForceLayout3D() extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  type Position = (Double, Double, Double)
  type PositionRDD = SortedRDD[ID, Position]

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val initialLayout = inputs.vs.rdd.mapValues(_ => (0.0, 0.0, 0.0))
    val undirected = inputs.es.rdd.flatMap {
      case (_, Edge(src, dst)) => Iterator(src -> dst, dst -> src)
    }.toSortedRDD(inputs.vs.rdd.partitioner.get)
    undirected.cache
    println(s"undirected: ${undirected.collect.toSeq}")
    val finalLayout = steps(initialLayout, undirected, 5)
    output(o.positions, finalLayout)
  }

  def edgePositions(ps: PositionRDD, es: SortedRDD[ID, ID]): SortedRDD[ID, (Position, Position)] = {
    val partitioner = ps.partitioner.get
    val bySrc = es.sortedJoin(ps).map {
      case (dst, (src, dstPos)) => src -> dstPos
    }.toSortedRDD(partitioner)
    ps.sortedJoin(bySrc)
  }

  def times(x: Double, p: Position): Position = (p._1 * x, p._2 * x, p._3 * x)
  def plus(p1: Position, p2: Position): Position = (p1._1 + p2._2, p1._2 + p2._2, p1._3 + p2._3)
  def len(p: Position): Double = Math.sqrt(p._1 * p._1 + p._2 * p._2 + p._3 * p._3)
  def randomVector(seed: Long): Position =
    ((seed % 5).toDouble / 4 - 0.5, (seed % 6).toDouble / 5 - 0.5, (seed % 6).toDouble / 6 - 0.5)

  val Gravity = 0.01
  val Attraction = 0.05
  val Repulsion = 0.05
  def improved(ps: PositionRDD, es: SortedRDD[ID, ID]): PositionRDD = {
    ps.cache
    println(s"ps ${ps.collect.toSeq}")
    val attraction = edgePositions(ps, es).mapValues {
      case (srcPos, dstPos) =>
        val d = plus(dstPos, times(-1.0, srcPos))
        times(Attraction, times(len(d), d))
    }.reduceByKey(plus _)
    // TODO: Do this without cartesian.
    val repulsion = ps.cartesian(ps.values).map {
      case ((src, srcPos), dstPos) =>
        val d = plus(srcPos, times(-1.0, dstPos))
        val l = len(d)
        val repu = if (l < 0.1) randomVector(src) else times(Repulsion / l / l, d)
        src -> repu
    }.toSortedRDD(ps.partitioner.get).reduceByKey(plus _)
    ps.sortedJoin(attraction.sortedJoin(repulsion)).mapValues {
      case (pos, (attr, repu)) => plus(plus(times(1.0 - Gravity, pos), attr), repu)
    }
  }

  @annotation.tailrec
  final def steps(ps: PositionRDD, es: SortedRDD[ID, ID], n: Int): PositionRDD =
    {
      println(s"step $n")
      if (n == 0) ps else steps(improved(ps, es), es, n - 1)
    }
}
