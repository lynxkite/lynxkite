package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json
import play.api.libs.json.Json

object TypedJson {
  def read[T <: ToJson](j: json.JsValue): T = {
    val cls = (j \ "class").as[String]
    val sym = reflect.runtime.currentMirror.staticModule(cls)
    val obj = reflect.runtime.currentMirror.reflectModule(sym).instance
    val des = obj.asInstanceOf[FromJson[T]]
    des.fromJson(j \ "data")
  }
}

trait ToJson {
  // Export blanks object by default.
  def toJson: json.JsValue = Json.obj()
  def toTypedJson: json.JsValue = Json.obj("class" -> getClass.getName, "data" -> toJson)
}
trait FromJson[T] {
  def fromJson(j: json.JsValue): T
}
trait OpFromJson extends FromJson[TypedMetaGraphOp[_ <: InputSignatureProvider, _ <: MetaDataSetProvider]]
