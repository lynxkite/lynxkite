// The filtering operation and all the specific filter classes that can be used.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Counters

abstract class Filter[-T] extends Serializable with ToJson {
  def matches(value: T): Boolean
}

object VertexAttributeFilter extends OpFromJson {
  class Output[T](implicit
      instance: MetaGraphOperationInstance,
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

  def execute(
    inputDatas: DataSet,
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

object EQ extends FromJson[EQ[_]] {
  def fromJson(j: JsValue) = EQ(TypedJson.read(j \ "exact"))
}

case class EQ[T](exact: T) extends Filter[T] {
  def matches(value: T) = value == exact
  override def toJson = Json.obj("exact" -> TypedJson(exact))
}

object DoubleEQ extends FromJson[EQ[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = {
    println(s"EQ Giving back $j")
    EQ((j \ "exact").as[Double])
  }
}

abstract class ComparatorFilter[T](base: T) extends Filter[T] {
  def lt(value: T): Boolean = {
    if (base.isInstanceOf[Double]) value.asInstanceOf[Double] < base.asInstanceOf[Double]
    else if (base.isInstanceOf[String]) value.asInstanceOf[String] < base.asInstanceOf[String]
    else if (base.isInstanceOf[Long]) value.asInstanceOf[Long] < base.asInstanceOf[Long]
    else throw new AssertionError(s"Value $value cannot be compared in a filter")
  }
  lt(base) // Type check as soon as created
  def lte(value: T) = lt(value) || value == base
  def gt(value: T) = !lte(value)
  def gte(value: T) = !lt(value)
}

object LT extends FromJson[LT[_]] {
  def fromJson(j: JsValue) = LT(TypedJson.read((j \ "bound")))
}

case class LT[T](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = lt(value)
  override def toJson = Json.obj("bound" -> TypedJson(bound))
}

object DoubleLT extends FromJson[LT[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = {
    println(s"LT Giving back $j")
    LT((j \ "bound").as[Double])
  }
}

object LE extends FromJson[LE[_]] {
  def fromJson(j: JsValue) = LE(TypedJson.read((j \ "bound")))
}
case class LE[T](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = lte(value)
  override def toJson = Json.obj("bound" -> TypedJson(bound))
}
object DoubleLE extends FromJson[LE[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = {
    println(s"LE Giving back $j")
    LE((j \ "bound").as[Double])
  }
}

object GT extends FromJson[GT[_]] {
  def fromJson(j: JsValue) = GT(TypedJson.read((j \ "bound")))
}
case class GT[T](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = gt(value)
  override def toJson = Json.obj("bound" -> TypedJson(bound))
}
object DoubleGT extends FromJson[GT[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = {
    println(s"GT Giving back $j")
    GT((j \ "bound").as[Double])
  }
}

object GE extends FromJson[GE[_]] {
  def fromJson(j: JsValue) = GE(TypedJson.read((j \ "bound")))
}
case class GE[T](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = gte(value)
  override def toJson = Json.obj("bound" -> TypedJson(bound))
}
object DoubleGE extends FromJson[GE[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = {
    println(s"GE Giving back $j")
    GE((j \ "bound").as[Double])
  }
}

object PairFilter extends FromJson[PairFilter[_, _]] {
  def fromJson(j: JsValue) =
    PairFilter(
      TypedJson.read[Filter[_]](j \ "filter1"),
      TypedJson.read[Filter[_]](j \ "filter2"))
}
case class PairFilter[T1, T2](filter1: Filter[T1], filter2: Filter[T2]) extends Filter[(T1, T2)] {
  def matches(value: (T1, T2)) = filter1.matches(value._1) && filter2.matches(value._2)
  override def toJson = Json.obj(
    "filter1" -> filter1.toTypedJson,
    "filter2" -> filter2.toTypedJson)
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
