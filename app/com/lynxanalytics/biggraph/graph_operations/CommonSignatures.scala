// Common operation input/output signatures.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import com.lynxanalytics.biggraph.graph_api._

class VertexAttributeInput[T] extends MagicInputSignature {
  val vs = vertexSet
  val attr = vertexAttribute[T](vs)
}

class ScalarInput[T] extends MagicInputSignature {
  val sc = scalar[T]
}

class GraphInput extends MagicInputSignature {
  val vs = vertexSet
  val es = edgeBundle(vs, vs)
}

class TableInput extends MagicInputSignature {
  val df = table
}

class Segmentation(
    vs: VertexSet,
    belongsToProperties: EdgeBundleProperties = EdgeBundleProperties.default)(
    implicit instance: MetaGraphOperationInstance)
    extends MagicOutput(instance) {
  val segments = vertexSet
  val belongsTo = edgeBundle(vs, segments, belongsToProperties)
}

class NoInput extends MagicInputSignature {}

class AttributeOutput[T: TypeTag](vs: VertexSet)(implicit instance: MetaGraphOperationInstance)
    extends MagicOutput(instance) {
  val attr = vertexAttribute[T](vs)
}

class ScalarOutput[T: TypeTag](implicit instance: MetaGraphOperationInstance)
    extends MagicOutput(instance) {
  val sc = scalar[T]
}

class TableOutput(schema: org.apache.spark.sql.types.StructType)(
    implicit instance: MetaGraphOperationInstance)
    extends MagicOutput(instance) {
  val t = table(schema)
}
