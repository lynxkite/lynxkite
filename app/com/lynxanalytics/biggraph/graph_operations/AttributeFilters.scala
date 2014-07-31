package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

abstract class Filter[T] extends Serializable {
  def matches(value: T): Boolean
}

object VertexAttributeFilter {
  class Input[T] extends MagicInputSignature {
    val vs = vertexSet
    val attr = vertexAttribute[T](vs)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    val fvs = vertexSet
    val identity = edgeBundle(inputs.vs.entity, fvs)
    implicit val tt = inputs.attr.typeTag
    val filteredAttribute = scalar[FilteredAttribute[T]]
  }
}
case class VertexAttributeFilter[T](filter: Filter[T])
    extends TypedMetaGraphOp[VertexAttributeFilter.Input[T], VertexAttributeFilter.Output[T]] {
  import VertexAttributeFilter._

  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val instance = output.instance
    implicit val tt = inputs.attr.data.typeTag
    implicit val ct = inputs.attr.data.classTag
    val attr = inputs.attr.rdd
    val fattr = attr.filter { case (id, v) => filter.matches(v) }
    output(o.fvs, fattr.mapValues(_ => ()).asSortedRDD)
    val identity = fattr.mapPartitions(
      {
        it => it.map { case (id, v) => id -> Edge(id, id) }
      },
      preservesPartitioning = true)
    output(o.identity, identity)
    output(o.filteredAttribute, FilteredAttribute(inputs.attr, filter))
  }
}

object EdgeAttributeFilter {
  class Input[T] extends MagicInputSignature {
    val srcVS = vertexSet
    val dstVS = vertexSet
    val eb = edgeBundle(srcVS, dstVS)
    val attr = edgeAttribute[T](eb)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    val feb = edgeBundle(inputs.srcVS.entity, inputs.dstVS.entity)
  }
}
case class EdgeAttributeFilter[T](filter: Filter[T])
    extends TypedMetaGraphOp[EdgeAttributeFilter.Input[T], EdgeAttributeFilter.Output[T]] {
  import EdgeAttributeFilter._

  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val tt = inputs.attr.data.typeTag
    implicit val ct = inputs.attr.data.classTag
    val attr = inputs.attr.rdd
    val fattr = attr.filter { case (id, v) => filter.matches(v) }.asSortedRDD
    output(
      o.feb,
      inputs.eb.rdd.sortedJoin(fattr).mapValues { case (edge, attr) => edge }.asSortedRDD)
  }
}

case class NotFilter[T](filter: Filter[T]) extends Filter[T] {
  def matches(value: T) = !filter.matches(value)
}
case class AndFilter[T](filters: Filter[T]*) extends Filter[T] {
  assert(filters.size > 0)
  def matches(value: T) = filters.forall(_.matches(value))
}

case class DoubleEQ(exact: Double) extends Filter[Double] {
  def matches(value: Double) = value == exact
}
case class DoubleLT(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value < bound
}
case class DoubleLE(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value <= bound
}
case class DoubleGT(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value > bound
}
case class DoubleGE(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value >= bound
}

case class StringOneOf(options: Set[String]) extends Filter[String] {
  def matches(value: String) = options.contains(value)
}
