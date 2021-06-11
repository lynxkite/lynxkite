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
  class GraphInput(hasWeight: Boolean, hasAttribute: Boolean) extends MagicInputSignature {
    val vs = vertexSet
    val es = edgeBundle(vs, vs)
    val weight = if (hasWeight) edgeAttribute[Double](es) else null
    val attr = if (hasAttribute) vertexAttribute[Double](vs) else null
  }
}

object NetworKitComputeDoubleAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeDoubleAttribute(
    (j \ "op").as[String],
    (j \ "hasWeight").as[Boolean],
    (j \ "hasAttribute").as[Boolean],
    (j \ "options").as[json.JsObject])
  def run(
      name: String,
      es: EdgeBundle,
      options: Map[String, Any] = Map(),
      weight: Option[Attribute[Double]] = None,
      attribute: Option[Attribute[Double]] = None)(
      implicit m: MetaGraphManager): Attribute[Double] = {
    val op = NetworKitComputeDoubleAttribute(
      name,
      weight.isDefined,
      attribute.isDefined,
      NetworKitCommon.toJson(options))
    import Scripting._
    var builder = op(op.es, es)
    if (weight.isDefined) { builder = builder(op.weight, weight.get) }
    if (attribute.isDefined) { builder = builder(op.attr, attribute.get) }
    builder.result.attr
  }
}
case class NetworKitComputeDoubleAttribute(
    op: String,
    hasWeight: Boolean,
    hasAttribute: Boolean,
    options: json.JsObject)
    extends TypedMetaGraphOp[NetworKitCommon.GraphInput, AttributeOutput[Double]] {
  @transient override lazy val inputs = new NetworKitCommon.GraphInput(hasWeight, hasAttribute)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Double](inputs.vs.entity)
  }
  override def toJson = json.Json.obj(
    "op" -> op,
    "hasWeight" -> hasWeight,
    "hasAttribute" -> hasAttribute,
    "options" -> options)
}

object NetworKitComputeDoubleEdgeAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeDoubleEdgeAttribute(
    (j \ "op").as[String],
    (j \ "hasWeight").as[Boolean],
    (j \ "hasAttribute").as[Boolean],
    (j \ "options").as[json.JsObject])
  def run(
      name: String,
      es: EdgeBundle,
      options: Map[String, Any] = Map(),
      weight: Option[Attribute[Double]] = None,
      attribute: Option[Attribute[Double]] = None)(
      implicit m: MetaGraphManager): Attribute[Double] = {
    val op = NetworKitComputeDoubleEdgeAttribute(
      name,
      weight.isDefined,
      attribute.isDefined,
      NetworKitCommon.toJson(options))
    import Scripting._
    var builder = op(op.es, es)
    if (weight.isDefined) { builder = builder(op.weight, weight.get) }
    if (attribute.isDefined) { builder = builder(op.attr, attribute.get) }
    builder.result.attr
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: NetworKitCommon.GraphInput)
      extends MagicOutput(instance) {
    val attr = edgeAttribute[Double](inputs.es.entity)
  }
}
case class NetworKitComputeDoubleEdgeAttribute(
    op: String,
    hasWeight: Boolean,
    hasAttribute: Boolean,
    options: json.JsObject)
    extends TypedMetaGraphOp[NetworKitCommon.GraphInput, NetworKitComputeDoubleEdgeAttribute.Output] {
  @transient override lazy val inputs = new NetworKitCommon.GraphInput(hasWeight, hasAttribute)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new NetworKitComputeDoubleEdgeAttribute.Output()(instance, inputs)
  }
  override def toJson = json.Json.obj(
    "op" -> op,
    "hasWeight" -> hasWeight,
    "hasAttribute" -> hasAttribute,
    "options" -> options)
}

object NetworKitComputeVectorAttribute extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeVectorAttribute(
    (j \ "op").as[String],
    (j \ "hasWeight").as[Boolean],
    (j \ "hasAttribute").as[Boolean],
    (j \ "options").as[json.JsObject])
  def run(
      name: String,
      es: EdgeBundle,
      options: Map[String, Any] = Map(),
      weight: Option[Attribute[Double]] = None,
      attribute: Option[Attribute[Double]] = None)(
      implicit m: MetaGraphManager): Attribute[Vector[Double]] = {
    val op = NetworKitComputeVectorAttribute(
      name,
      weight.isDefined,
      attribute.isDefined,
      NetworKitCommon.toJson(options))
    import Scripting._
    var builder = op(op.es, es)
    if (weight.isDefined) { builder = builder(op.weight, weight.get) }
    if (attribute.isDefined) { builder = builder(op.attr, attribute.get) }
    builder.result.attr
  }
}
case class NetworKitComputeVectorAttribute(
    op: String,
    hasWeight: Boolean,
    hasAttribute: Boolean,
    options: json.JsObject)
    extends TypedMetaGraphOp[NetworKitCommon.GraphInput, AttributeOutput[Vector[Double]]] {
  @transient override lazy val inputs = new NetworKitCommon.GraphInput(hasWeight, hasAttribute)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Vector[Double]](inputs.vs.entity)
  }
  override def toJson = json.Json.obj(
    "op" -> op,
    "hasWeight" -> hasWeight,
    "hasAttribute" -> hasAttribute,
    "options" -> options)
}

object NetworKitCreateGraph extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitCreateGraph(
    (j \ "op").as[String],
    (j \ "options").as[json.JsObject])
  def run(name: String, options: Map[String, Any] = Map())(
      implicit m: MetaGraphManager): Output = {
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
    (j \ "op").as[String],
    (j \ "hasWeight").as[Boolean],
    (j \ "hasAttribute").as[Boolean],
    (j \ "options").as[json.JsObject])
  def run(
      name: String,
      es: EdgeBundle,
      options: Map[String, Any] = Map(),
      weight: Option[Attribute[Double]] = None,
      attribute: Option[Attribute[Double]] = None)(
      implicit m: MetaGraphManager): Output = {
    val op = NetworKitCommunityDetection(
      name,
      weight.isDefined,
      attribute.isDefined,
      NetworKitCommon.toJson(options))
    import Scripting._
    var builder = op(op.es, es)
    if (weight.isDefined) { builder = builder(op.weight, weight.get) }
    if (attribute.isDefined) { builder = builder(op.attr, attribute.get) }
    builder.result
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: NetworKitCommon.GraphInput)
      extends MagicOutput(instance) {
    val partitions = vertexSet
    val belongsTo = edgeBundle(
      inputs.vs.entity,
      partitions,
      properties = EdgeBundleProperties.partialFunction)
  }
}
case class NetworKitCommunityDetection(
    op: String,
    hasWeight: Boolean,
    hasAttribute: Boolean,
    options: json.JsObject)
    extends TypedMetaGraphOp[NetworKitCommon.GraphInput, NetworKitCommunityDetection.Output] {
  @transient override lazy val inputs = new NetworKitCommon.GraphInput(hasWeight, hasAttribute)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new NetworKitCommunityDetection.Output()(instance, inputs)
  override def toJson = json.Json.obj(
    "op" -> op,
    "hasWeight" -> hasWeight,
    "hasAttribute" -> hasAttribute,
    "options" -> options)
}

object NetworKitComputeScalar extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeScalar(
    (j \ "op").as[String],
    (j \ "hasWeight").as[Boolean],
    (j \ "hasAttribute").as[Boolean],
    (j \ "options").as[json.JsObject])
  def run(
      name: String,
      es: EdgeBundle,
      options: Map[String, Any] = Map(),
      weight: Option[Attribute[Double]] = None,
      attribute: Option[Attribute[Double]] = None)(
      implicit m: MetaGraphManager): Output = {
    val op = NetworKitComputeScalar(
      name,
      weight.isDefined,
      attribute.isDefined,
      NetworKitCommon.toJson(options))
    import Scripting._
    var builder = op(op.es, es)
    if (weight.isDefined) { builder = builder(op.weight, weight.get) }
    if (attribute.isDefined) { builder = builder(op.attr, attribute.get) }
    builder.result
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: NetworKitCommon.GraphInput)
      extends MagicOutput(instance) {
    val scalar1 = scalar[Double]
    val scalar2 = scalar[Double]
  }
}
case class NetworKitComputeScalar(
    op: String,
    hasWeight: Boolean,
    hasAttribute: Boolean,
    options: json.JsObject)
    extends TypedMetaGraphOp[NetworKitCommon.GraphInput, NetworKitComputeScalar.Output] {
  @transient override lazy val inputs = new NetworKitCommon.GraphInput(hasWeight, hasAttribute)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new NetworKitComputeScalar.Output()(instance, inputs)
  override def toJson = json.Json.obj(
    "op" -> op,
    "hasWeight" -> hasWeight,
    "hasAttribute" -> hasAttribute,
    "options" -> options)
}

object NetworKitComputeSegmentAttribute extends OpFromJson {
  class Input(hasWeight: Boolean, hasAttribute: Boolean) extends MagicInputSignature {
    val vs = vertexSet
    val segments = vertexSet
    val es = edgeBundle(vs, vs)
    val belongsTo = edgeBundle(vs, segments)
    val weight = if (hasWeight) edgeAttribute[Double](es) else null
    val attr = if (hasAttribute) vertexAttribute[Double](vs) else null
  }
  def fromJson(j: json.JsValue) = NetworKitComputeSegmentAttribute(
    (j \ "op").as[String],
    (j \ "hasWeight").as[Boolean],
    (j \ "hasAttribute").as[Boolean],
    (j \ "options").as[json.JsObject])
  // Returns an attribute for the segments.
  def run(
      name: String,
      es: EdgeBundle,
      belongsTo: EdgeBundle,
      options: Map[String, Any] = Map(),
      weight: Option[Attribute[Double]] = None,
      attribute: Option[Attribute[Double]] = None)(
      implicit m: MetaGraphManager): Attribute[Double] = {
    val op = NetworKitComputeSegmentAttribute(
      name,
      weight.isDefined,
      attribute.isDefined,
      NetworKitCommon.toJson(options))
    import Scripting._
    var builder = op(op.es, es)(op.belongsTo, belongsTo)
    if (weight.isDefined) { builder = builder(op.weight, weight.get) }
    if (attribute.isDefined) { builder = builder(op.attr, attribute.get) }
    builder.result.attr
  }
}
case class NetworKitComputeSegmentAttribute(
    op: String,
    hasWeight: Boolean,
    hasAttribute: Boolean,
    options: json.JsObject)
    extends TypedMetaGraphOp[NetworKitComputeSegmentAttribute.Input, AttributeOutput[Double]] {
  @transient override lazy val inputs = new NetworKitComputeSegmentAttribute.Input(hasWeight, hasAttribute)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Double](inputs.segments.entity)
  }
  override def toJson = json.Json.obj(
    "op" -> op,
    "hasWeight" -> hasWeight,
    "hasAttribute" -> hasAttribute,
    "options" -> options)
}

object NetworKitComputeSegmentationScalar extends OpFromJson {
  def fromJson(j: json.JsValue) = NetworKitComputeSegmentationScalar(
    (j \ "op").as[String],
    (j \ "hasWeight").as[Boolean],
    (j \ "hasAttribute").as[Boolean],
    (j \ "options").as[json.JsObject])
  // Returns an attribute for the segments.
  def run(
      name: String,
      es: EdgeBundle,
      belongsTo: EdgeBundle,
      options: Map[String, Any] = Map(),
      weight: Option[Attribute[Double]] = None,
      attribute: Option[Attribute[Double]] = None)(
      implicit m: MetaGraphManager): Scalar[Double] = {
    val op = NetworKitComputeSegmentationScalar(
      name,
      weight.isDefined,
      attribute.isDefined,
      NetworKitCommon.toJson(options))
    import Scripting._
    var builder = op(op.es, es)(op.belongsTo, belongsTo)
    if (weight.isDefined) { builder = builder(op.weight, weight.get) }
    if (attribute.isDefined) { builder = builder(op.attr, attribute.get) }
    builder.result.sc
  }
}
case class NetworKitComputeSegmentationScalar(
    op: String,
    hasWeight: Boolean,
    hasAttribute: Boolean,
    options: json.JsObject)
    extends TypedMetaGraphOp[NetworKitComputeSegmentAttribute.Input, ScalarOutput[Double]] {
  @transient override lazy val inputs = new NetworKitComputeSegmentAttribute.Input(hasWeight, hasAttribute)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new ScalarOutput[Double]
  }
  override def toJson = json.Json.obj(
    "op" -> op,
    "hasWeight" -> hasWeight,
    "hasAttribute" -> hasAttribute,
    "options" -> options)
}
