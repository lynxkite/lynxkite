// Utility classes used in the serialization of the metagraph.
package com.lynxanalytics.lynxkite.graph_api

import play.api.libs.json
import play.api.libs.json.{Writes, Reads}
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

// TypedJson is a JSON object with a string "class" and an object "data" field:
//   { "class": "my.little.ClassName", "data": { ... } }
// This format allows reading objects whose exact type is not known in advance.
// For example Operations are stored like this.
//
// For this to work, the object's class (T) must extend the ToJson trait and it
// must have a companion object that extends FromJson[T].
object TypedJson {
  // Re-creates the object from a TypedJson format.
  def read[T](jr: json.JsReadable): T = {
    val j = jr.as[json.JsObject]
    try {
      (j \ "class").as[String] match {
        case "Long" => (j \ "data").as[Long].asInstanceOf[T]
        case "Double" => (j \ "data").as[Double].asInstanceOf[T]
        case "String" => (j \ "data").as[String].asInstanceOf[T]
        case cls =>
          // Find the companion object.
          val sym = reflect.runtime.currentMirror.staticModule(cls)
          val obj = reflect.runtime.currentMirror.reflectModule(sym).instance
          val des = obj.asInstanceOf[FromJson[T]]
          // Ask the companion object to parse the data.
          des.fromJson((j \ "data").get)
      }
    } catch {
      // Include more details in the exception.
      case e: Throwable => throw new Exception(s"Failed to read $j", e)
    }
  }

  // Creates TypedJson for supported types.
  def apply(p: Any): json.JsValue = {
    p match {
      case p: Long => json.Json.obj("class" -> "Long", "data" -> p)
      case p: Double => json.Json.obj("class" -> "Double", "data" -> p)
      case p: String => json.Json.obj("class" -> "String", "data" -> p)
      case p: ToJson => p.toTypedJson
    }
  }

  def createFromWriter[T: json.Writes](p: T): json.JsValue = {
    json.Json.obj("class" -> p.getClass.getName, "data" -> json.Json.toJson(p))
  }
}

// Extend ToJson if you want to be serializable in this system.
trait ToJson {
  // Export a blank object by default. Override this.
  def toJson: json.JsValue = Json.obj()
  // Convenient shorthand to access Json.
  protected def Json = json.Json
  // Create TypedJson representation.
  def toTypedJson: json.JsValue = Json.obj("class" -> getClass.getName, "data" -> toJson)
}

// Extend FromJson[T] in T's companion object.
trait FromJson[+T] {
  // Convenient shorthand.
  protected type JsValue = json.JsValue
  // Re-creates an object from a JSON input. Override this.
  def fromJson(j: json.JsValue): T
}

// Operation companion objects should extend OpFromJson.
trait OpFromJson extends FromJson[TypedMetaGraphOp.Type]

// Class to support json reads and writes for newly introduced
// parameters. In these cases, we want to preserve compatibility,
// so we don't serialize the default value,
// and the lack of the relevant field is interpreted as the
// default value at deserialization. As a result, deserialization
// will work for legacy data (the default value will be used), as well
// as for new data.
case class NewParameter[T: Writes: Reads](paramName: String, defaultValue: T) {

  def toJson(valueToSave: T): json.JsObject = {
    if (defaultValue == valueToSave) {
      json.Json.obj()
    } else {
      json.Json.obj(paramName -> valueToSave)
    }
  }

  def fromJson(jr: json.JsReadable): T = {
    val j = jr.as[json.JsObject]
    (j \ paramName).asOpt[T].getOrElse(defaultValue)
  }
}

object SerializableType {
  object TypeParser {
    import fastparse.all._
    type PS = P[SerializableType[_]]
    val string = P("String").map(_ => SerializableType.string)
    val double = P("Double").map(_ => SerializableType.double)
    val long = P("Long").map(_ => SerializableType.long)
    val id = P("ID").map(_ => SerializableType.id)
    val int = P("Int").map(_ => SerializableType.int)
    val timestamp = P("Timestamp").map(_ => SerializableType.timestamp)
    val date = P("Date").map(_ => SerializableType.date)
    val boolean = P("Boolean").map(_ => SerializableType.boolean)
    val primitive = P(string | double | long | int | id | timestamp | date | boolean)
    val vector: PS = P("Vector[" ~ stype ~ "]").map {
      inner => SerializableType.vector(inner)
    }
    val tuple2: PS = P("Tuple2[" ~ stype ~ "," ~ stype ~ "]").map {
      case (inner1, inner2) => SerializableType.tuple2(inner1, inner2)
    }
    val stype: PS = P(primitive | vector | tuple2)
    def parse(s: String) = {
      val fastparse.core.Parsed.Success(result, _) = stype.parse(s)
      result
    }
  }
  def fromJson(jr: json.JsReadable): SerializableType[_] = {
    val j = jr.as[json.JsObject]
    TypeParser.parse((j \ "typename").as[String])
  }

  val string = new SerializableType[String]("String")
  val double = new SerializableType[Double]("Double")
  val id = new SerializableType[com.lynxanalytics.lynxkite.graph_api.ID]("ID")
  val long = new SerializableType[Long]("Long")
  val int = new SerializableType[Int]("Int")
  val timestamp = {
    implicit val o = new TimestampOrdering
    implicit val f = TypeTagToFormat.formatTimestamp
    new SerializableType[java.sql.Timestamp]("Timestamp")
  }
  val date = {
    implicit val o = new DateOrdering
    implicit val f = TypeTagToFormat.formatDate
    new SerializableType[java.sql.Date]("Date")
  }
  val boolean = new SerializableType[Boolean]("Boolean")

  // Custom orderings.
  class TimestampOrdering extends Ordering[java.sql.Timestamp] with Serializable {
    def compare(x: java.sql.Timestamp, y: java.sql.Timestamp): Int = x.compareTo(y)
  }
  class DateOrdering extends Ordering[java.sql.Date] with Serializable {
    def compare(x: java.sql.Date, y: java.sql.Date): Int = x.compareTo(y)
  }
  // Orderings for types that should throw an error for comparisons.
  class UnsupportedVectorOrdering[T: TypeTag] extends Ordering[Vector[T]] with Serializable {
    def compare(x: Vector[T], y: Vector[T]): Int = ???
  }
  class UnsupportedTuple2Ordering[T1: TypeTag, T2: TypeTag] extends Ordering[(T1, T2)] with Serializable {
    def compare(x: (T1, T2), y: (T1, T2)): Int = ???
  }

  def vector[T](innerType: SerializableType[T]): SerializableType[Vector[T]] = {
    new VectorSerializableType(s"Vector[${innerType.getTypename}]")(innerType.typeTag)
  }

  def tuple2[A, B](innerType1: SerializableType[A], innerType2: SerializableType[B]): SerializableType[(A, B)] = {
    new Tuple2SerializableType(s"Tuple2[${innerType1.getTypename},${innerType2.getTypename}]")(
      innerType1.typeTag,
      innerType2.typeTag)
  }

  def apply[T: TypeTag]: SerializableType[T] = {
    apply(typeOf[T]).asInstanceOf[SerializableType[T]]
  }

  def apply(t: Type): SerializableType[_] = {
    if (t =:= typeOf[String]) string
    else if (t =:= typeOf[Double]) double
    // ID must come before Long, because Long would match too
    // But we have to use the stricter == comparison
    else if (t == typeOf[com.lynxanalytics.lynxkite.graph_api.ID]) id
    else if (t =:= typeOf[Long]) long
    else if (t =:= typeOf[Int]) int
    else if (t =:= typeOf[Boolean]) boolean
    else if (TypeTagUtil.isOfKind2[Tuple2](t)) tuple2(
      apply(t.asInstanceOf[TypeRefApi].args(0)),
      apply(t.asInstanceOf[TypeRefApi].args(1)))
    else if (TypeTagUtil.isOfKind1[Vector](t)) vector(apply(t.asInstanceOf[TypeRefApi].args(0)))
    else throw new AssertionError(s"Unsupported type: $t")
  }

  object Implicits {
    implicit def classTag[T](implicit st: SerializableType[T]) = st.classTag
    implicit def format[T](implicit st: SerializableType[T]) = st.format
    implicit def ordering[T](implicit st: SerializableType[T]) = st.ordering
    implicit def typeTag[T](implicit st: SerializableType[T]) = st.typeTag
  }
}
class SerializableType[T] private[graph_api] (
    typename: String)(implicit
    val classTag: ClassTag[T],
    // @transient drops this non-serializable field when sending to the executors.
    @transient val format: play.api.libs.json.Format[T],
    val ordering: Ordering[T],
    val typeTag: TypeTag[T])
    extends ToJson with Serializable {
  override def toJson = Json.obj("typename" -> typename)
  def getTypename = typename
  override def equals(o: Any) = o match {
    case t: SerializableType[_] => this.typeTag.tpe =:= t.typeTag.tpe
    case _ => false
  }
}
class VectorSerializableType[T: TypeTag] private[graph_api] (
    typename: String)
    extends SerializableType[Vector[T]](typename)(
      classTag = RuntimeSafeCastable.classTagFromTypeTag(typeTag),
      format = TypeTagToFormat.vectorToFormat(typeTag),
      ordering = new SerializableType.UnsupportedVectorOrdering()(typeTag),
      typeTag = TypeTagUtil.vectorTypeTag(typeTag)) {}
class Tuple2SerializableType[T1: TypeTag, T2: TypeTag] private[graph_api] (
    typename: String)
    extends SerializableType[(T1, T2)](typename)(
      classTag = RuntimeSafeCastable.classTagFromTypeTag(typeTag),
      format = TypeTagToFormat.pairToFormat(typeTag[T1], typeTag[T2]),
      ordering = new SerializableType.UnsupportedTuple2Ordering()(typeTag[T1], typeTag[T2]),
      typeTag = TypeTagUtil.tuple2TypeTag(typeTag[T1], typeTag[T2])) {}
