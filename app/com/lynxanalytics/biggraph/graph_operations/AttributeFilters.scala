package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Counters
import com.lynxanalytics.biggraph.spark_util.Implicits._

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
  def fromJson(j: play.api.libs.json.JsValue) = {
    VertexAttributeFilter[Nothing](TypedJson.read[Filter[Nothing]](j \ "filter"))
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

object NotFilter extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) =
    NotFilter[Nothing](TypedJson.read[Filter[Nothing]](j \ "filter"))
}
case class NotFilter[T](filter: Filter[T]) extends Filter[T] {
  def matches(value: T) = !filter.matches(value)
  override def toJson = Json.obj("filter" -> filter.toTypedJson)
}

object AndFilter extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = {
    val filters = (j \ "filters").as[Seq[play.api.libs.json.JsValue]].map(j => TypedJson.read[Filter[Nothing]](j))
    AndFilter[Nothing](filters: _*)
  }
}
case class AndFilter[T](filters: Filter[T]*) extends Filter[T] {
  assert(filters.size > 0)
  def matches(value: T) = filters.forall(_.matches(value))
  override def toJson = {
    Json.obj("filters" -> play.api.libs.json.JsArray(filters.map(_.toTypedJson)))
  }
}

object DoubleEQ extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = DoubleEQ((j \ "exact").as[Double])
}
case class DoubleEQ(exact: Double) extends Filter[Double] {
  def matches(value: Double) = value == exact
  override def toJson = Json.obj("exact" -> exact)
}

object DoubleLT extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = DoubleLT((j \ "bound").as[Double])
}
case class DoubleLT(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value < bound
  override def toJson = Json.obj("bound" -> bound)
}

object DoubleLE extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = DoubleLE((j \ "bound").as[Double])
}
case class DoubleLE(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value <= bound
  override def toJson = Json.obj("bound" -> bound)
}

object DoubleGT extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = DoubleGT((j \ "bound").as[Double])
}
case class DoubleGT(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value > bound
  override def toJson = Json.obj("bound" -> bound)
}

object DoubleGE extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = DoubleGE((j \ "bound").as[Double])
}
case class DoubleGE(bound: Double) extends Filter[Double] {
  def matches(value: Double) = value >= bound
  override def toJson = Json.obj("bound" -> bound)
}

object OneOf extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = {
    (j \ "type").as[String] match {
      case "Long" => OneOf((j \ "options").as[Set[Long]])
      case "String" => OneOf((j \ "options").as[Set[String]])
    }
  }
}
case class OneOf[T: reflect.ClassTag](options: Set[T]) extends Filter[T] {
  def matches(value: T) = options.contains(value)
  override def toJson = {
    val LongTag = reflect.classTag[Long]
    val StringTag = reflect.classTag[String]
    reflect.classTag[T] match {
      case LongTag => Json.obj("type" -> "Long", "options" -> options.toSeq.asInstanceOf[Seq[Long]])
      case StringTag => Json.obj("type" -> "String", "options" -> options.toSeq.asInstanceOf[Seq[String]])
    }
  }
}

object Exists extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) =
    Exists[Nothing](TypedJson.read[Filter[Nothing]](j \ "filter"))
}
case class Exists[T](filter: Filter[T]) extends Filter[Vector[T]] {
  def matches(value: Vector[T]) = value.exists(filter.matches(_))
  override def toJson = Json.obj("filter" -> filter.toTypedJson)
}

object ForAll extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) =
    ForAll[Nothing](TypedJson.read[Filter[Nothing]](j \ "filter"))
}
case class ForAll[T](filter: Filter[T]) extends Filter[Vector[T]] {
  def matches(value: Vector[T]) = value.forall(filter.matches(_))
  override def toJson = Json.obj("filter" -> filter.toTypedJson)
}

object PairEquals extends FromJson[Filter[Nothing]] {
  def fromJson(j: play.api.libs.json.JsValue) = PairEquals[Nothing]()
}
case class PairEquals[T]() extends Filter[(T, T)] {
  def matches(value: (T, T)) = value._1 == value._2
}
