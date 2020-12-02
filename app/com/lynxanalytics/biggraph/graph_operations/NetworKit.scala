// Sphynx-only operations that use NetworKit.
// Since NetworKit has so many algorithms we try to avoid having to add new
// classes for each of them.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json
import com.lynxanalytics.biggraph.graph_api._

object NetworKitCommon {
  def toJson(m: Map[String, Any]) = {
    json.JsObject(m.mapValues {
      case v: String => json.Json.toJson(v)
      case v: Int => json.Json.toJson(v)
      case v: Long => json.Json.toJson(v)
      case v: Double => json.Json.toJson(v)
      case v: Boolean => json.Json.toJson(v)
    }.toSeq)
  }
}

object NetworKitComputeAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeAttribute(
    (j \ "op").as[String], (j \ "options").as[json.JsObject])
  def run(name: String, es: EdgeBundle, options: Map[String, Any] = Map())(
    implicit
    m: MetaGraphManager): Attribute[Double] = {
    val op = NetworKitComputeAttribute(name, NetworKitCommon.toJson(options))
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

object NetworKitComputeVectorAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeVectorAttribute(
    (j \ "op").as[String], (j \ "options").as[json.JsObject])
  def run(name: String, es: EdgeBundle, options: Map[String, Any] = Map())(
    implicit
    m: MetaGraphManager): Attribute[Vector[Double]] = {
    val op = NetworKitComputeVectorAttribute(name, NetworKitCommon.toJson(options))
    import Scripting._
    op(op.es, es).result.attr
  }
}
case class NetworKitComputeVectorAttribute(op: String, options: json.JsObject)
  extends TypedMetaGraphOp[GraphInput, AttributeOutput[Vector[Double]]] {
  @transient override lazy val inputs = new GraphInput()
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Vector[Double]](inputs.vs.entity)
  }
  override def toJson = json.Json.obj("op" -> op, "options" -> options)
}

object NetworKitCreateGraph extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitCreateGraph(
    (j \ "op").as[String], (j \ "options").as[json.JsObject])
  def run(name: String, options: Map[String, Any] = Map())(
    implicit
    m: MetaGraphManager): Output = {
    val op = NetworKitCreateGraph(name, NetworKitCommon.toJson(options))
    import Scripting._
    op().result
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
    val es = edgeBundle(vs, vs)
  }
}
case class NetworKitCreateGraph(op: String, options: json.JsObject)
  extends TypedMetaGraphOp[NoInput, NetworKitCreateGraph.Output] {
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new NetworKitCreateGraph.Output()(instance)
  override def toJson = json.Json.obj("op" -> op, "options" -> options)
}
