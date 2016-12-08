// Common operation input/output signatures.
package com.lynxanalytics.biggraph.graph_operations

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

class Segmentation(
  vs: VertexSet, belongsToProperties: EdgeBundleProperties = EdgeBundleProperties.default)(
    implicit instance: MetaGraphOperationInstance)
    extends MagicOutput(instance) {
  val segments = vertexSet
  val belongsTo = edgeBundle(vs, segments, belongsToProperties)
}

class NoInput extends MagicInputSignature {}
