package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._

abstract class Filter[T] extends Serializable {
  def matches(value: T): Boolean
}

object VertexAttributeFilter {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T]) extends MagicOutput(instance) {
    val fvs = vertexSet
    val identity = edgeBundle(inputs.vs.entity, fvs)
  }
}
import VertexAttributeFilter._
case class VertexAttributeFilter[T](filter: Filter[T])
    extends TypedMetaGraphOp[VertexAttributeInput[T], Output[T]] {
  @transient override lazy val inputs = new VertexAttributeInput[T]

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
    val fattr = attr.filter { case (id, v) => filter.matches(v) }
    output(o.fvs, fattr.mapValues(_ => ()))
    val identity = fattr.mapValuesWithKeys({ case (id, v) => Edge(id, id) })
    output(o.identity, identity)
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
