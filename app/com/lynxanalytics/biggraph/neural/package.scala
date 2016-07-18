// Type aliases for neural network code.
package com.lynxanalytics.biggraph

import breeze.linalg._
import com.lynxanalytics.biggraph.graph_api.ID

package object neural {
  type DoubleVector = DenseVector[Double]
  type DoubleMatrix = DenseMatrix[Double]
  type Graph[T] = Map[ID, T]
  type GraphData = Graph[DoubleVector]
  type GraphVectors = Graph[Iterable[DoubleVector]]
}
