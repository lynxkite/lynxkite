// Sphynx-only operations that use NetworKit.
// Since NetworKit has so many algorithms we try to avoid having to add new
// classes for each of them.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json
import com.lynxanalytics.biggraph.graph_api._

object NetworKitComputeAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeAttribute(
    (j \ "op").as[String], (j \ "options").as[json.JsObject])
  def run(name: String, es: EdgeBundle, options: Map[String, Any] = Map())(
    implicit
    m: MetaGraphManager): Attribute[Double] = {
    val j = json.JsObject(options.mapValues {
      case v: String => json.Json.toJson(v)
      case v: Int => json.Json.toJson(v)
      case v: Double => json.Json.toJson(v)
      case v: Boolean => json.Json.toJson(v)
    }.toSeq)
    val op = NetworKitComputeAttribute(name, j)
    import Scripting._
    op(op.es, es).result.attr
  }
}
case class NetworKitComputeAttribute(op: String, options: json.JsObject)
  extends TypedMetaGraphOp[GraphInput, AttributeOutput[Double]] {
  @transient override lazy val inputs = new GraphInput()
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Double](inputs.vs.entity)
  }
  override def toJson = json.Json.obj("op" -> op, "options" -> options)
}
