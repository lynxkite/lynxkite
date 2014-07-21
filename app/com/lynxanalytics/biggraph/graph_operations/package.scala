package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.graph_api._

package object graph_operations {

  class EdgeAttributeInput[T] extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val es = edgeBundle(src, dst)
    val attr = edgeAttribute[T](es)
  }

  class VertexAttributeInput[T] extends MagicInputSignature {
    val vs = vertexSet
    val attr = vertexAttribute[T](vs)
  }

}
