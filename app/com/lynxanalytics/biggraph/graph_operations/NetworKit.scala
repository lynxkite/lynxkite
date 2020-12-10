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
  class WeightedGraphInput(weighted: Boolean) extends MagicInputSignature {
    val vs = vertexSet
    val es = edgeBundle(vs, vs)
    val weight = if (weighted) edgeAttribute[Double](es) else null
  }
}

object NetworKitComputeAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeAttribute(
    (j \ "op").as[String], (j \ "weighted").as[Boolean], (j \ "options").as[json.JsObject])
  def run(
    name: String,
    es: EdgeBundle,
    options: Map[String, Any] = Map(),
    weight: Option[Attribute[Double]] = None)(
    implicit
    m: MetaGraphManager): Attribute[Double] = {
    val op = NetworKitComputeAttribute(name, weight.isDefined, NetworKitCommon.toJson(options))
    import Scripting._
    weight match {
      case Some(weight) => op(op.es, es)(op.weight, weight).result.attr
      case None => op(op.es, es).result.attr
    }
  }
}
case class NetworKitComputeAttribute(op: String, weighted: Boolean, options: json.JsObject)
  extends TypedMetaGraphOp[NetworKitCommon.WeightedGraphInput, AttributeOutput[Double]] {
  @transient override lazy val inputs = new NetworKitCommon.WeightedGraphInput(weighted)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Double](inputs.vs.entity)
  }
  override def toJson = json.Json.obj("op" -> op, "weighted" -> weighted, "options" -> options)
}

object NetworKitComputeVectorAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeVectorAttribute(
    (j \ "op").as[String], (j \ "weighted").as[Boolean], (j \ "options").as[json.JsObject])
  def run(
    name: String,
    es: EdgeBundle,
    options: Map[String, Any] = Map(),
    weight: Option[Attribute[Double]] = None)(
    implicit
    m: MetaGraphManager): Attribute[Vector[Double]] = {
    val op = NetworKitComputeVectorAttribute(name, weight.isDefined, NetworKitCommon.toJson(options))
    import Scripting._
    weight match {
      case Some(weight) => op(op.es, es)(op.weight, weight).result.attr
      case None => op(op.es, es).result.attr
    }
  }
}
case class NetworKitComputeVectorAttribute(op: String, weighted: Boolean, options: json.JsObject)
  extends TypedMetaGraphOp[NetworKitCommon.WeightedGraphInput, AttributeOutput[Vector[Double]]] {
  @transient override lazy val inputs = new NetworKitCommon.WeightedGraphInput(weighted)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Vector[Double]](inputs.vs.entity)
  }
  override def toJson = json.Json.obj("op" -> op, "weighted" -> weighted, "options" -> options)
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

object NetworKitCommunityDetection extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitCommunityDetection(
    (j \ "op").as[String], (j \ "weighted").as[Boolean], (j \ "options").as[json.JsObject])
  def run(
    name: String,
    es: EdgeBundle,
    options: Map[String, Any] = Map(),
    weight: Option[Attribute[Double]] = None)(
    implicit
    m: MetaGraphManager): Output = {
    val op = NetworKitCommunityDetection(name, weight.isDefined, NetworKitCommon.toJson(options))
    import Scripting._
    weight match {
      case Some(weight) => op(op.es, es)(op.weight, weight).result
      case None => op(op.es, es).result
    }
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: NetworKitCommon.WeightedGraphInput) extends MagicOutput(instance) {
    val partitions = vertexSet
    val belongsTo = edgeBundle(
      inputs.vs.entity, partitions, properties = EdgeBundleProperties.partialFunction)
  }
}
case class NetworKitCommunityDetection(op: String, weighted: Boolean, options: json.JsObject)
  extends TypedMetaGraphOp[NetworKitCommon.WeightedGraphInput, NetworKitCommunityDetection.Output] {
  @transient override lazy val inputs = new NetworKitCommon.WeightedGraphInput(weighted)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new NetworKitCommunityDetection.Output()(instance, inputs)
  override def toJson = json.Json.obj("op" -> op, "weighted" -> weighted, "options" -> options)
}

object NetworKitComputeScalar extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeScalar(
    (j \ "op").as[String], (j \ "weighted").as[Boolean], (j \ "options").as[json.JsObject])
  def run(
    name: String,
    es: EdgeBundle,
    options: Map[String, Any] = Map(),
    weight: Option[Attribute[Double]] = None)(
    implicit
    m: MetaGraphManager): Output = {
    val op = NetworKitComputeScalar(name, weight.isDefined, NetworKitCommon.toJson(options))
    import Scripting._
    weight match {
      case Some(weight) => op(op.es, es)(op.weight, weight).result
      case None => op(op.es, es).result
    }
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: NetworKitCommon.WeightedGraphInput) extends MagicOutput(instance) {
    val scalar1 = scalar[Double]
    val scalar2 = scalar[Double]
  }
}
case class NetworKitComputeScalar(op: String, weighted: Boolean, options: json.JsObject)
  extends TypedMetaGraphOp[NetworKitCommon.WeightedGraphInput, NetworKitComputeScalar.Output] {
  @transient override lazy val inputs = new NetworKitCommon.WeightedGraphInput(weighted)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new NetworKitComputeScalar.Output()(instance, inputs)
  override def toJson = json.Json.obj("op" -> op, "weighted" -> weighted, "options" -> options)
}
