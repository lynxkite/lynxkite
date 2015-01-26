package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json

// TypedJson if a JSON object with a string "class" and an object "data" field:
//   { "class": "my.little.ClassName", "data": { ... } }
// This format allows reading objects whose exact type is not known in advance.
// For example Operations are stored like this.
//
// For this to work, the object's class (T) must extend the ToJson trait and it
// must have a companion object that extends FromJson[T].
object TypedJson {
  // Re-creates the object from a TypedJson format.
  def read[T <: ToJson](j: json.JsValue): T = {
    // Find the companion object.
    val cls = (j \ "class").as[String]
    val sym = reflect.runtime.currentMirror.staticModule(cls)
    val obj = reflect.runtime.currentMirror.reflectModule(sym).instance
    val des = obj.asInstanceOf[FromJson[T]]
    // Ask the companion object to parse the data.
    des.fromJson(j \ "data")
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
