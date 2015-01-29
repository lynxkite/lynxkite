package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json

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
