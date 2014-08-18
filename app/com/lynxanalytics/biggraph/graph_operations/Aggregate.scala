package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object AggregateByEdgeBundle {
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
import AggregateByEdgeBundle._
case class AggregateByEdgeBundle[From, To](aggregator: Aggregator[From, To])
    extends TypedMetaGraphOp[Input[From], Output[From, To]] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input[From]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    val tt = aggregator.outputTypeTag(inputs.attr.typeTag)
    new Output[From, To]()(tt, instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag

    val bySrc = inputs.connection.rdd.map {
      case (id, edge) => edge.src -> edge.dst
    }.toSortedRDD(inputs.src.rdd.partitioner.get)
    val withAttr = bySrc.sortedJoin(inputs.attr.rdd)
    val byDst = withAttr.map {
      case (src, (dst, attr)) => dst -> attr
    }
    val grouped = byDst.groupBySortedKey(inputs.dst.rdd.partitioner.get)
    val aggregated = grouped.mapValues(aggregator.aggregate(_))
    output(o.attr, aggregated)
  }
}

trait Aggregator[From, To] {
  def outputTypeTag(inputTypeTag: TypeTag[From]): TypeTag[To]
  // aggregate() can assume that values is non-empty.
  def aggregate(values: Iterable[From]): To
}
object Aggregator {
  case class Count[T]() extends Aggregator[T, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = typeTag[Double]
    def aggregate(values: Iterable[T]): Double = values.size
  }

  case class Sum() extends Aggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def aggregate(values: Iterable[Double]): Double = values.sum
  }

  case class Average() extends Aggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def aggregate(values: Iterable[Double]): Double = values.sum / values.size
  }

  case class First[T]() extends Aggregator[T, T] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = inputTypeTag
    def aggregate(values: Iterable[T]): T = values.head
  }
}
