package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Counters
import com.lynxanalytics.biggraph.spark_util.Implicits._

abstract class Filter[-T] extends Serializable {
  def matches(value: T): Boolean
}
object Filter {
  import play.api.libs.json
  import play.api.libs.json.Json
  implicit val fFilter = new json.Format[Filter[Any]] {
    def writes(f: Filter[Any]) = Json.obj()
    def reads(js: json.JsValue): json.JsResult[Filter[Any]] = {
      json.JsSuccess(DoubleEQ(111).asInstanceOf[Filter[Any]])
    }
  }
}

object VertexAttributeFilter extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T]) extends MagicOutput(instance) {
    val fvs = vertexSet
    val identity = edgeBundle(fvs, inputs.vs.entity, EdgeBundleProperties.embedding)
    implicit val tt = inputs.attr.typeTag
    val filteredAttribute = scalar[FilteredAttribute[T]]
  }
  def fromJson(j: play.api.libs.json.JsValue) = {
    import Filter.fFilter
    VertexAttributeFilter[Any]((j \ "filter").as[Filter[Any]])
  }
}
case class VertexAttributeFilter[T](filter: Filter[T])
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeFilter.Output[T]] {
  import VertexAttributeFilter._

  @transient override lazy val inputs = new VertexAttributeInput[T]

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

    val filteringSessionName = "filtering into: %s [%s]".format(o.fvs.entity, o.fvs.entity.gUID)
    val tried = Counters.newCounter(
      "Trials while " + filteringSessionName,
      rc.sparkContext)
    val matched = Counters.newCounter(
      "Matches while " + filteringSessionName,
      rc.sparkContext)

    val fattr = attr.filter {
      case (id, v) =>
        val res = filter.matches(v)
        tried += 1
        if (res) matched += 1
        res
    }
    output(o.fvs, fattr.mapValues(_ => ()))
    val identity = fattr.mapValuesWithKeys { case (id, _) => Edge(id, id) }
    output(o.identity, identity)
    output(o.filteredAttribute, FilteredAttribute(inputs.attr, filter))
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

case class OneOf[T](options: Set[T]) extends Filter[T] {
  def matches(value: T) = options.contains(value)
}

case class Exists[T](filter: Filter[T]) extends Filter[Vector[T]] {
  def matches(value: Vector[T]) = value.exists(filter.matches(_))
}

case class ForAll[T](filter: Filter[T]) extends Filter[Vector[T]] {
  def matches(value: Vector[T]) = value.forall(filter.matches(_))
}

case class PairEquals[T]() extends Filter[(T, T)] {
  def matches(value: (T, T)) = value._1 == value._2
}
