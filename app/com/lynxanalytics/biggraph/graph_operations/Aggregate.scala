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
case class AggregateByEdgeBundle[From, To](aggregator: LocalAggregator[From, To])
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

trait LocalAggregator[From, To] {
  def outputTypeTag(inputTypeTag: TypeTag[From]): TypeTag[To]
  // aggregate() can assume that values is non-empty.
  def aggregate(values: Iterable[From]): To
}
trait Aggregator[From, Intermediate, To] extends LocalAggregator[From, To] {
  def zero: Intermediate
  def merge(a: Intermediate, b: From): Intermediate
  def combine(a: Intermediate, b: Intermediate): Intermediate
  def finalize(i: Intermediate): To
  def aggregate(values: Iterable[From]): To =
    finalize(values.foldLeft(zero)(merge _))
}
trait SimpleAggregator[From, To] extends Aggregator[From, To, To] {
  def finalize(i: To): To = i
}
// This is a trait instead of an abstract class because otherwise the case
// class will not be serializable ("no valid constructor").
trait CompoundAggregator[From, Intermediate1, Intermediate2, To1, To2, To]
    extends Aggregator[From, (Intermediate1, Intermediate2), To] {
  val agg1: Aggregator[From, Intermediate1, To1]
  val agg2: Aggregator[From, Intermediate2, To2]
  def zero = (agg1.zero, agg2.zero)
  def merge(a: (Intermediate1, Intermediate2), b: From) =
    (agg1.merge(a._1, b), agg2.merge(a._2, b))
  def combine(a: (Intermediate1, Intermediate2), b: (Intermediate1, Intermediate2)) =
    (agg1.combine(a._1, b._1), agg2.combine(a._2, b._2))
  def finalize(i: (Intermediate1, Intermediate2)): To =
    compound(agg1.finalize(i._1), agg2.finalize(i._2))
  def compound(res1: To1, res2: To2): To
}

object Aggregator {
  case class Count[T]() extends SimpleAggregator[T, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: T) = a + 1
    def combine(a: Double, b: Double) = a + b
  }

  case class Sum() extends SimpleAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: Double) = a + b
    def combine(a: Double, b: Double) = a + b
  }

  case class Average() extends CompoundAggregator[Double, Double, Double, Double, Double, Double] {
    val agg1 = Count[Double]()
    val agg2 = Sum()
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def compound(count: Double, sum: Double) = sum / count
  }

  case class MostCommon[T]() extends LocalAggregator[T, T] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = inputTypeTag
    def aggregate(values: Iterable[T]) = {
      values.groupBy(identity).maxBy(_._2.size)._1
    }
  }

  // Majority is like MostCommon, but returns "" if the mode is < fraction of the values.
  case class Majority(fraction: Double) extends LocalAggregator[String, String] {
    def outputTypeTag(inputTypeTag: TypeTag[String]) = typeTag[String]
    def aggregate(values: Iterable[String]) = {
      val (mode, count) = values.groupBy(identity).mapValues(_.size).maxBy(_._2)
      if (count >= fraction * values.size) mode else ""
    }
  }

  case class First[T]() extends Aggregator[T, Option[T], T] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = inputTypeTag
    def zero = None
    def merge(a: Option[T], b: T) = a.orElse(Some(b))
    def combine(a: Option[T], b: Option[T]) = a.orElse(b)
    def finalize(opt: Option[T]) = opt.get
  }
}
