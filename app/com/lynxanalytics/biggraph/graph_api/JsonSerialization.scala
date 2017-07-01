// Utility classes used in the serialization of the metagraph.
package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json
import play.api.libs.json.{ Writes, Reads }
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
  def read[T](j: json.JsValue): T = {
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
          des.fromJson(j \ "data")
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

  def fromJson(j: json.JsValue): T = {
    (j \ paramName).asOpt[T].getOrElse(defaultValue)
  }
}

object SerializableType {
  def fromJson(j: json.JsValue): SerializableType[_] = {
    (j \ "typename").as[String] match {
      case "String" => string
      case "Double" => double
      case "Long" => long
      case "Int" => int
      case "Vector[String]" => stringVector
      case "Vector[Double]" => doubleVector
    }
  }

  val string = new SerializableType[String]("String")
  val double = new SerializableType[Double]("Double")
  val long = new SerializableType[Long]("Long")
  val int = new SerializableType[Int]("Int")

  class MockVectorOrdering[T] extends Ordering[Vector[T]] with Serializable {
    def compare(x: Vector[T], y: Vector[T]): Int = ???
  }

  implicit val oSV = new MockVectorOrdering[String]
  val stringVector = new SerializableType[Vector[String]]("Vector[String]")
  implicit val oDV = new MockVectorOrdering[Double]
  val doubleVector = new SerializableType[Vector[Double]]("Vector[Double]")

  def apply[T: TypeTag]: SerializableType[T] = {
    import scala.language.existentials
    val t = typeOf[T]
    val st =
      if (t =:= typeOf[String]) string
      else if (t =:= typeOf[Double]) double
      else if (t =:= typeOf[Long]) long
      else if (t =:= typeOf[Int]) int
      else if (t =:= typeOf[Vector[String]]) stringVector
      else if (t =:= typeOf[Vector[Double]]) doubleVector
      else assert(false, s"Unsupported type: $t")
    st.asInstanceOf[SerializableType[T]]
  }

  object Implicits {
    implicit def classTag[T](implicit st: SerializableType[T]) = st.classTag
    implicit def format[T](implicit st: SerializableType[T]) = st.format
    implicit def ordering[T](implicit st: SerializableType[T]) = st.ordering
    implicit def typeTag[T](implicit st: SerializableType[T]) = st.typeTag
  }
}
class SerializableType[T] private (typename: String)(implicit val classTag: ClassTag[T],
                                                     val format: play.api.libs.json.Format[T],
                                                     val ordering: Ordering[T],
                                                     val typeTag: TypeTag[T]) extends ToJson {
  override def toJson = Json.obj("typename" -> typename)
}
