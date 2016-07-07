// Collection of classes for building a neural network.
package com.lynxanalytics.biggraph.graph_operations

import breeze.linalg._
import com.lynxanalytics.biggraph.graph_api._

trait NetworkOutputs

object NeuralGates {
  class Vector {
    def *(m: M) = MultiplyWeights(this, m)
    def *(v: Vector) = MultiplyElements(this, v)
    def *(s: Double) = MultiplyScalar(this, s)
    def unary_- = this * (-1)
    def +(v: Vector) = AddElements(this, v)
    def -(v: Vector) = AddElements(this, -v)
    def +(v: V) = AddWeights(this, v)
  }
  class Vectors {
    def *(m: Matrix) = MultiplyVectors(this, m)
  }
  case class MultiplyWeights(v: Vector, w: M) extends Vector
  case class MultiplyScalar(v: Vector, s: Double) extends Vector
  case class MultiplyElements(v1: Vector, v2: Vector) extends Vector
  case class AddElements(v1: Vector, v2: Vector) extends Vector
  case class AddWeights(v: Vector, w: V) extends Vector
  case class MultiplyVector(v: Vector, m: M) extends Vector
  case class MultiplyVectors(vs: Vectors, m: M) extends Vectors
  case class Sum(vs: Vectors) extends Vector
  case class Neighbors() extends Vectors
  case class Input(name: String) extends Vector
  case class Sigmoid(v: Vector) extends Vector
  case class Tanh(v: Vector) extends Vector
  // Trained vector or matrix of weights.
  case class V(name: String)
  case class M(name: String)
  // Output declaration.
  type GraphData = Map[ID, DenseVector[Double]]
  case class Outputs(outputs: (String, Vector)*) {
    def forward(
      vertices: Seq[ID],
      edges: CompactUndirectedGraph,
      neighbors: GraphData,
      inputs: (String, GraphData)*): NetworkOutputs = ???

    def backward(
      vertices: Seq[ID],
      edges: CompactUndirectedGraph,
      outputs: NetworkOutputs,
      gradients: (String, GraphData)*): GraphData = ???

    def update(gradients: Iterable[GraphData]): Outputs = ???
  }
}
