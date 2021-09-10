// The filtering operation and all the specific filter classes that can be used.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Counters

abstract class Filter[-T] extends Serializable with ToJson {
  def matches(value: T): Boolean
}

object VertexAttributeFilter extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
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
    extends SparkOperation[VertexAttributeInput[T], VertexAttributeFilter.Output[T]] {
  import VertexAttributeFilter._

  override val isHeavy = true
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
  def fromJson(j: JsValue) = EQ((j \ "exact").as[Double])
}

import SerializableType.Implicits._

trait ComparatorFilterObject[Comp] extends FromJson[Comp] {
  def fromJson(j: JsValue) = {
    fromType(j)(SerializableType.fromJson((j \ "type")))
  }
  private def fromType[Inner](j: JsValue)(implicit st: SerializableType[Inner]) = {
    import st._
    fromBound((j \ "bound").as[Inner])
  }
  def fromBound[Inner: SerializableType](bound: Inner): Comp
}
abstract class ComparatorFilter[T: SerializableType](bound: T) extends Filter[T] {
  val st = implicitly[SerializableType[T]]
  implicit val ops = st.ordering.mkOrderingOps _
  def matches(value: T): Boolean
  override def toJson = Json.obj("bound" -> bound, "type" -> implicitly[SerializableType[T]].toJson)
}

object LT extends ComparatorFilterObject[LT[_]] {
  override def fromBound[Inner: SerializableType](bound: Inner) = LT(bound)
}
case class LT[T: SerializableType](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = value < bound
}
object DoubleLT extends FromJson[LT[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = LT((j \ "bound").as[Double])(SerializableType[Double])
}

object LE extends ComparatorFilterObject[LE[_]] {
  override def fromBound[Inner: SerializableType](bound: Inner) = LE(bound)
}
case class LE[T: SerializableType](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = value <= bound
}
object DoubleLE extends FromJson[LE[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = LE((j \ "bound").as[Double])(SerializableType[Double])
}

object GT extends ComparatorFilterObject[GT[_]] {
  override def fromBound[Inner: SerializableType](bound: Inner) = GT(bound)
}
case class GT[T: SerializableType](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = value > bound
}
object DoubleGT extends FromJson[GT[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = GT((j \ "bound").as[Double])(SerializableType[Double])
}

object GE extends ComparatorFilterObject[GE[_]] {
  override def fromBound[Inner: SerializableType](bound: Inner) = GE(bound)
}
case class GE[T: SerializableType](bound: T) extends ComparatorFilter[T](bound) {
  def matches(value: T) = value >= bound
}
object DoubleGE extends FromJson[GE[Double]] { // Backward compatibility
  def fromJson(j: JsValue) = GE((j \ "bound").as[Double])(SerializableType[Double])
}

object VectorFilter extends FromJson[VectorFilter[_]] {
  def fromJson(j: JsValue) = {
    val filters = (j \ "filters").as[Seq[JsValue]].map(j => TypedJson.read[Filter[_]](j))
    VectorFilter(filters: _*)
  }
}
case class VectorFilter[T](filters: Filter[T]*) extends Filter[Vector[T]] {
  def matches(value: Vector[T]) = filters.zip(value).forall { case (f, v) => f.matches(v) }
  override def toJson =
    Json.obj("filters" -> play.api.libs.json.JsArray(filters.map(_.toTypedJson)))
}

object OneOf extends FromJson[OneOf[_]] {
  def fromJson(j: JsValue) = {
    val set = (j \ "options").as[Set[JsValue]].map(TypedJson.read[Any](_))
    OneOf(set)
  }
}
case class OneOf[T](options: Set[T]) extends Filter[T] {
  def matches(value: T) = options.contains(value)
  override def toJson = Json.obj("options" -> options.toSeq.map(TypedJson(_)).sortBy(_.toString))
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
