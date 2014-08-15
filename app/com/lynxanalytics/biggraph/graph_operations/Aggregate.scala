package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object Aggregate {
  class Input[From] extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val connection = edgeBundle(src, dst)
    val attr = vertexAttribute[From](src)
  }
  class Output[From, To: TypeTag](implicit instance: MetaGraphOperationInstance,
                                  inputs: Input[From]) extends MagicOutput(instance) {
    val attr = vertexAttribute[To](inputs.dst.entity)
  }
}
import Aggregate._
abstract class Aggregate[From, To] extends TypedMetaGraphOp[Input[From], Output[From, To]] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input[From]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[From, To]()(tt, instance, inputs)

  def tt: TypeTag[To]
  def aggregate(values: Iterable[From]): To

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag

    val bySrc = inputs.connection.rdd.map {
      case (id, edge) => edge.src -> edge.dst
    }.partitionBy(inputs.src.rdd.partitioner.get)
    val withAttr = bySrc.join(inputs.attr.rdd)
    val byDst = withAttr.map {
      case (src, (dst, attr)) => dst -> attr
    }
    val grouped = byDst.groupByKey(inputs.dst.rdd.partitioner.get)
    val aggregated = grouped.mapValues(aggregate _)
    output(o.attr, aggregated.toSortedRDD)
  }
}

case class AggregateByCount[T]() extends Aggregate[T, Double] {
  def tt = typeTag[Double]
  override def aggregate(values: Iterable[T]): Double = values.size
}

case class AggregateBySum() extends Aggregate[Double, Double] {
  def tt = typeTag[Double]
  override def aggregate(values: Iterable[Double]): Double = values.sum
}

case class AggregateByAverage() extends Aggregate[Double, Double] {
  def tt = typeTag[Double]
  override def aggregate(values: Iterable[Double]): Double = values.sum / values.size
}
