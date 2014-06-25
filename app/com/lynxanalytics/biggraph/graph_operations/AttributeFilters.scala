package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._

abstract class Filter[T] extends Serializable with RuntimeSafeCastable[T, Filter] {
  def typeTag: TypeTag[T]
  def matches(value: T): Boolean
}

case class VertexAttributeFilter[T](filter: Filter[T]) extends MetaGraphOperation {
  def signature = {
    implicit val tt = filter.typeTag
    newSignature
      .inputVertexAttribute[T]('attr, 'vs, create = true)
      .outputVertexSet('fvs)
      .outputEdgeBundle('projection, 'vs -> 'fvs)
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    implicit val tt = filter.typeTag
    implicit val ct = filter.classTag
    val attr = inputs.vertexAttributes('attr).runtimeSafeCast[T].rdd
    val fattr = attr.filter { case (id, v) => filter.matches(v) }
    outputs.putVertexSet('fvs, fattr.mapValues(_ => ()))
    val projection = fattr.map({ case (id, v) => id -> Edge(id, id) }).partitionBy(attr.partitioner.get)
    outputs.putEdgeBundle('projection, projection)
  }
}

case class NotFilter[T](filter: Filter[T]) extends Filter[T] {
  def typeTag = filter.typeTag
  def matches(value: T) = !filter.matches(value)
}
case class AndFilter[T](filters: Filter[T]*) extends Filter[T] {
  assert(filters.size > 0)
  def typeTag = filters.head.typeTag
  def matches(value: T) = filters.forall(_.matches(value))
}

abstract class DoubleFilter extends Filter[Double] {
  @transient lazy val typeTag = DoubleFilter.tt
}
object DoubleFilter {
  val tt = typeTag[Double]
}

case class DoubleEQ(exact: Double) extends DoubleFilter {
  def matches(value: Double) = value == exact
}
case class DoubleLT(bound: Double) extends DoubleFilter {
  def matches(value: Double) = value < bound
}
case class DoubleLE(bound: Double) extends DoubleFilter {
  def matches(value: Double) = value <= bound
}
case class DoubleGT(bound: Double) extends DoubleFilter {
  def matches(value: Double) = value > bound
}
case class DoubleGE(bound: Double) extends DoubleFilter {
  def matches(value: Double) = value >= bound
}

abstract class StringFilter extends Filter[String] {
  @transient lazy val typeTag = StringFilter.tt
}
object StringFilter {
  val tt = typeTag[String]
}

case class StringOneOf(options: Set[String]) extends StringFilter {
  def matches(value: String) = options.contains(value)
}
