// Type aliases for neural network code.
package com.lynxanalytics.biggraph

import breeze.linalg._
import com.lynxanalytics.biggraph.graph_api.ID

package object neural {
  type DoubleVector = DenseVector[Double]
  type DoubleMatrix = DenseMatrix[Double]
  type Graph[T] = Map[ID, T]
  type VectorGraph = Graph[DoubleVector]
  type VectorsGraph = Graph[Iterable[DoubleVector]]
}
