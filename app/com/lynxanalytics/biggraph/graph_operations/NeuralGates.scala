// Collection of classes for building a neural network.
package com.lynxanalytics.biggraph.graph_operations

import breeze.linalg._
import breeze.stats.distributions.RandBasis
import com.lynxanalytics.biggraph.graph_api._

trait NetworkOutputs {
  def apply(output: String): NeuralGates.GraphData
}

object NeuralGates {
  trait NetworkElement extends Product {
    // Finds all instances of a type in the network leading up to this node.
    protected def find[T: reflect.ClassTag]: Seq[T] = {
      this.productIterator.flatMap {
        case x if (reflect.classTag[T].runtimeClass.isInstance(x)) => Some(x.asInstanceOf[T])
        case x: NetworkElement => x.find[T]
        case _ => None
      }.toSeq
    }
  }

  trait Vector extends NetworkElement {
    def *(m: M) = MultiplyWeights(this, m)
    def *(v: Vector) = MultiplyElements(this, v)
    def *(s: Double) = MultiplyScalar(this, s)
    def unary_- = this * (-1)
    def +(v: Vector) = AddElements(this, v)
    def -(v: Vector) = AddElements(this, -v)
    def +(v: V) = AddWeights(this, v)
    // Plain toString on case classes is enough to uniquely identify vectors.
    private[NeuralGates] def id = this.toString
    private[NeuralGates] def forward(ctx: ForwardContext): GraphData
  }

  class Vectors extends NetworkElement {
    def *(m: M) = MultiplyVectors(this, m)
  }

  case class MultiplyWeights(v: Vector, w: M) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }
  case class MultiplyScalar(v: Vector, s: Double) extends Vector {
    def forward(ctx: ForwardContext) = s * ctx(v)
  }
  case class MultiplyElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v1) :* ctx(v2)
  }
  case class AddElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) + ctx(v)
  }
  case class AddWeights(v: Vector, w: V) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) + ctx(v)
  }
  case class MultiplyVector(v: Vector, m: M) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }
  case class MultiplyVectors(vs: Vectors, m: M) extends Vectors {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }
  case class Sum(vs: Vectors) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }
  case class Neighbors() extends Vectors {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }
  case class Input(name: String) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }
  case class Sigmoid(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }
  case class Tanh(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(w) * ctx(v)
  }

  // Trained vector or matrix of weights.
  trait Trained[T] {
    def name: String
    def random(size: Int)(implicit r: RandBasis): T
  }
  val initialRandomAmplitude = 0.01
  case class V(name: String) extends Trained[DenseVector[Double]] {
    def random(size: Int)(implicit r: RandBasis) =
      randn(size) * initialRandomAmplitude
  }
  case class M(name: String) extends Trained[DenseMatrix[Double]] {
    def random(size: Int)(implicit r: RandBasis) =
      randn((size, size)) * initialRandomAmplitude
  }

  type GraphData = Map[ID, DenseVector[Double]]

  private case class ForwardContext(network: Network, inputs: Map[String, GraphData]) {
    val cache = collection.mutable.Map[String, GraphData]()
    def apply(v: Vector): GraphData = {
      cache.getOrElseUpdate(v.id, v match {
        case Input(name) => inputs(name)
        case v => v.forward(this)
      })
    }
    def apply[T](t: Trained[T]) = network(t)
  }

  object Network {
    // The public constructor does not set weights, so they get randomly initialized.
    def apply(size: Int, outputs: (String, Vector)*) = new Network(size, Map(), outputs.toMap)
  }
  case class Network private(
    size: Int, weights: Map[String, Trained[_]], outputs: Map[String, Vector]) {
    implicit val randBasis = RandBasis.mt0
    def apply[T](t: Trained[T]): T = weights.getOrElse(t.name, t.random(size)).asInstanceOf[T]
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

    def update(gradients: Iterable[GraphData]): Network = ???
  }
}
