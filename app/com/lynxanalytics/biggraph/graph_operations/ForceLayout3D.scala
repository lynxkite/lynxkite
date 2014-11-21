package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object ForceLayout3D {
  class Input extends GraphInput {
    val degree = vertexAttribute[Double](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val positions = vertexAttribute[(Double, Double, Double)](inputs.vs.entity)
  }
}
import ForceLayout3D._
case class ForceLayout3D() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  type Position = (Double, Double, Double)
  type PositionRDD = SortedRDD[ID, Position]
  type WeightedEdgeRDD = SortedRDD[ID, (ID, Double)] // src -> (dst, dstWeight)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = inputs.vs.rdd.partitioner.get
    val initialLayout = inputs.vs.rdd.mapValues(_ => (0.0, 0.0, 0.0))
    val undirected = inputs.es.rdd.flatMap {
      case (_, Edge(src, dst)) => Iterator(src -> dst, dst -> src)
    }.toSortedRDD(partitioner)
    val weighted = undirected.sortedJoin(inputs.degree.rdd).map {
      case (src, (dst, srcDegree)) => dst -> (src, srcDegree)
    }.toSortedRDD(partitioner)
    weighted.cache
    println(s"weighted: ${weighted.collect.toSeq}")
    val finalLayout = steps(initialLayout, weighted, 50)
    output(o.positions, finalLayout)
  }

  def edgePositions(ps: PositionRDD, es: WeightedEdgeRDD): SortedRDD[ID, (Double, Position, Position)] = {
    val partitioner = ps.partitioner.get
    val bySrc = es.sortedJoin(ps).map {
      case (dst, ((src, srcWeight), dstPos)) => src -> (srcWeight, dstPos)
    }.toSortedRDD(partitioner)
    ps.sortedJoin(bySrc).mapValues {
      case (srcPos, (srcWeight, dstPos)) => (srcWeight, srcPos, dstPos)
    }
  }

  implicit class VectorOps(v: Position) {
    def *(x: Double): Position = (v._1 * x, v._2 * x, v._3 * x)
    def /(x: Double): Position = v * (1.0 / x)
    def +(v2: Position): Position = (v._1 + v2._1, v._2 + v2._2, v._3 + v2._3)
    def -(v2: Position): Position = (v._1 - v2._1, v._2 - v2._2, v._3 - v2._3)
    def len: Double = Math.sqrt(v._1 * v._1 + v._2 * v._2 + v._3 * v._3)
  }
  def randomVector(seed: Long): Position = {
    val phase = seed.toDouble
    println(s"randomVector: $seed")
    (Math.sin(phase), Math.sin(phase * 2.0), Math.sin(phase * 3.0))
  }

  final val Gravity = 0.01
  final val IdealDistance = 1.0
  final val Fraction = 0.001
  def improved(ps: PositionRDD, es: WeightedEdgeRDD): PositionRDD = {
    ps.cache
    println(s"ps ${ps.collect.toSeq}")
    val attraction = edgePositions(ps, es).mapValues {
      case (srcWeight, srcPos, dstPos) =>
        val d = dstPos - srcPos
        d * d.len * Fraction / IdealDistance / srcWeight
    }.reduceByKey(_ + _)
    // TODO: Do this without cartesian.
    val repulsion = ps.cartesian(ps).flatMap {
      case ((src, srcPos), (dst, dstPos)) => if (src == dst) None else {
        val d = srcPos - dstPos
        val l = d.len
        val repu = if (l < 0.1) randomVector(src) else d * Fraction * IdealDistance * IdealDistance / l / l
        Some(src -> repu)
      }
    }.toSortedRDD(ps.partitioner.get).reduceByKey(_ + _)
    ps.sortedJoin(attraction.sortedJoin(repulsion)).mapValues {
      case (pos, (attr, repu)) =>
        println(s"pos: $pos, attr: $attr, repu: $repu")
        pos * (1.0 - Gravity) + attr + repu
    }
  }

  @annotation.tailrec
  final def steps(ps: PositionRDD, es: WeightedEdgeRDD, n: Int): PositionRDD =
    {
      println(s"step $n")
      if (n == 0) ps else steps(improved(ps, es), es, n - 1)
    }
}
