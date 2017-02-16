// The filtering operation and all the specific filter classes that can be used.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Counters

abstract class Filter[-T] extends Serializable with ToJson {
  def matches(value: T): Boolean
}

object VertexAttributeFilter extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T]) extends MagicOutput(instance) {
    val fvs = vertexSet
    val identity = edgeBundle(fvs, inputs.vs.entity, EdgeBundleProperties.embedding)
    implicit val tt = inputs.attr.typeTag
    val filteredAttribute = scalar[FilteredAttribute[T]]
  }
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    VertexAttributeFilter(TypedJson.read[Filter[_]](j \ "filter"))
  }
}
case class VertexAttributeFilter[T](filter: Filter[T])
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeFilter.Output[T]] {
  import VertexAttributeFilter._

  @transient override lazy val inputs = new VertexAttributeInput[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)
  override def toJson = Json.obj("filter" -> filter.toTypedJson)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val instance = output.instance
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
        tried.add(1)
        if (res) matched.add(1)
        res
    }
    output(o.fvs, fattr.mapValues(_ => ()))
    val identity = fattr.mapValuesWithKeys { case (id, _) => Edge(id, id) }
    output(o.identity, identity)
    output(o.filteredAttribute, FilteredAttribute(inputs.attr, filter))
  }
}

object MatchAllFilter extends FromJson[MatchAllFilter[_]] {
  def fromJson(j: JsValue) = MatchAllFilter()
}
case class MatchAllFilter[T]() extends Filter[T] {
  def matches(value: T) = true
}

object NotFilter extends FromJson[NotFilter[_]] {
  def fromJson(j: JsValue) =
    NotFilter(TypedJson.read[Filter[_]](j \ "filter"))
}
case class NotFilter[T](filter: Filter[T]) extends Filter[T] {
  def matches(value: T) = !filter.matches(value)
  override def toJson = Json.obj("filter" -> filter.toTypedJson)
}

object AndFilter extends FromJson[AndFilter[_]] {
  def fromJson(j: JsValue) = {
    val filters = (j \ "filters").as[Seq[JsValue]].map(j => TypedJson.read[Filter[_]](j))
    AndFilter(filters: _*)
  }
}
case class AndFilter[T](filters: Filter[T]*) extends Filter[T] {
  assert(filters.size > 0, "Cannot take the conjunction of 0 filters.")
  def matches(value: T) = filters.forall(_.matches(value))
  override def toJson = {
    Json.obj("filters" -> play.api.libs.json.JsArray(filters.map(_.toTypedJson)))
  }
}

object DoubleEQ extends FromJson[DoubleEQ] {
  def fromJson(j: JsValue) = DoubleEQ((j \ "exact").as[Double])
}
case class DoubleEQ(exact: Double) extends Filter[Double] {
  def matches(value: Double) = value == exact
  override def toJson = Json.obj("exact" -> exact)
}

object DoubleLT extends FromJson[DoubleLT] {
  def fromJson(j: JsValue) = DoubleLT((j \ "bound").as[Double])
}
case class DoubleLT(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value < bound
  override def toJson = Json.obj("bound" -> bound)
}

object DoubleLE extends FromJson[DoubleLE] {
  def fromJson(j: JsValue) = DoubleLE((j \ "bound").as[Double])
}
case class DoubleLE(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value <= bound
  override def toJson = Json.obj("bound" -> bound)
}

object DoubleGT extends FromJson[DoubleGT] {
  def fromJson(j: JsValue) = DoubleGT((j \ "bound").as[Double])
}
case class DoubleGT(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value > bound
  override def toJson = Json.obj("bound" -> bound)
}

object DoubleGE extends FromJson[DoubleGE] {
  def fromJson(j: JsValue) = DoubleGE((j \ "bound").as[Double])
}
case class DoubleGE(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value >= bound
  override def toJson = Json.obj("bound" -> bound)
}

object GeoFilter extends FromJson[GeoFilter] {
  def fromJson(j: JsValue) = GeoFilter(
    AndFilter.fromJson(j \ "latFilter").asInstanceOf[AndFilter[Double]],
    AndFilter.fromJson(j \ "lonFilter").asInstanceOf[AndFilter[Double]])
}
case class GeoFilter(latFilter: AndFilter[Double], lonFilter: AndFilter[Double]) extends Filter[(Double, Double)] {
  def matches(value: (Double, Double)) = latFilter.matches(value._1) && lonFilter.matches(value._2)
  override def toJson = Json.obj("latFilter" -> latFilter.toJson, "lonFilter" -> lonFilter.toJson)
}

object OneOf extends FromJson[OneOf[_]] {
  def fromJson(j: JsValue) = {
    val set = (j \ "options").as[Set[JsValue]].map(TypedJson.read[Any](_))
    OneOf(set)
  }
}
case class OneOf[T](options: Set[T]) extends Filter[T] {
  def matches(value: T) = options.contains(value)
  override def toJson = Json.obj("options" -> options.toSeq.map(TypedJson(_)))
}

object Exists extends FromJson[Exists[_]] {
  def fromJson(j: JsValue) =
    Exists(TypedJson.read[Filter[_]](j \ "filter"))
}
case class Exists[T](filter: Filter[T]) extends Filter[Vector[T]] {
  def matches(value: Vector[T]) = value.exists(filter.matches(_))
  override def toJson = Json.obj("filter" -> filter.toTypedJson)
}

object ForAll extends FromJson[ForAll[_]] {
  def fromJson(j: JsValue) =
    ForAll(TypedJson.read[Filter[_]](j \ "filter"))
}
case class ForAll[T](filter: Filter[T]) extends Filter[Vector[T]] {
  def matches(value: Vector[T]) = value.forall(filter.matches(_))
  override def toJson = Json.obj("filter" -> filter.toTypedJson)
}

object PairEquals extends FromJson[PairEquals[_]] {
  def fromJson(j: JsValue) = PairEquals()
}
case class PairEquals[T]() extends Filter[(T, T)] {
  def matches(value: (T, T)) = value._1 == value._2
}

object RegexFilter extends FromJson[RegexFilter] {
  def fromJson(j: JsValue) = RegexFilter((j \ "pattern").as[String])
}
case class RegexFilter(pattern: String) extends Filter[String] {
  val regex = new util.matching.Regex(pattern)
  def matches(value: String) = regex.findFirstIn(value).nonEmpty
  override def toJson = Json.obj("pattern" -> pattern)
}
