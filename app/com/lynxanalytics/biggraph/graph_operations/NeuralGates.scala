// Collection of classes for building a neural network.
package com.lynxanalytics.biggraph.graph_operations

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.RandBasis
import com.lynxanalytics.biggraph.graph_api._

trait NetworkOutputs {
  def apply(output: String): NeuralGates.GraphData
}

object NeuralGates {
  type DoubleVector = DenseVector[Double]
  type DoubleMatrix = DenseMatrix[Double]
  type Graph[T] = Map[ID, T]
  type GraphData = Graph[DoubleVector]
  type GraphVectors = Graph[Iterable[DoubleVector]]

  trait NetworkElement[Output] extends Product {
    // Finds all instances of a type in the network leading up to this node.
    protected def find[T: reflect.ClassTag]: Seq[T] = {
      this.productIterator.flatMap {
        case x if (reflect.classTag[T].runtimeClass.isInstance(x)) => Some(x.asInstanceOf[T])
        case x: NetworkElement[_] => x.find[T]
        case _ => None
      }.toSeq
    }
    // Plain toString on case classes is enough to uniquely identify vectors.
    private[NeuralGates] def id = this.toString
    private[NeuralGates] def forward(ctx: ForwardContext): Output
  }

  trait Vector extends NetworkElement[GraphData] {
    def *(m: M) = MultiplyWeights(this, m)
    def *(v: Vector) = MultiplyElements(this, v)
    def *(s: Double) = MultiplyScalar(this, s)
    def unary_- = this * (-1)
    def +(v: Vector) = AddElements(this, v)
    def -(v: Vector) = AddElements(this, -v)
    def +(v: V) = AddWeights(this, v)
  }

  trait Vectors extends NetworkElement[GraphVectors] {
    def *(m: M) = MultiplyVectors(this, m)
  }

  case class MultiplyWeights(v: Vector, w: M) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(ctx(w) * _)
  }
  case class MultiplyScalar(v: Vector, s: Double) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(s * _)
  }
  case class MultiplyElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v1, v2).mapValues { case (v1, v2) => v1 :* v2 }
  }
  case class AddElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v1, v2).mapValues { case (v1, v2) => v1 + v2 }
  }
  case class AddWeights(v: Vector, w: V) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(ctx(w) + _)
  }
  case class MultiplyVectors(vs: Vectors, w: M) extends Vectors {
    def forward(ctx: ForwardContext) = ctx(vs).mapValues(_.map(ctx(w) * _))
  }
  case class Sum(vs: Vectors) extends Vector {
    def forward(ctx: ForwardContext) = ctx(vs).mapValues { vectors =>
      val sum = DenseVector.zeros[Double](ctx.network.size)
      for (v <- vectors) sum += v
      sum
    }
  }
  case class Neighbors() extends Vectors {
    def forward(ctx: ForwardContext) = ctx.neighbors
  }
  case class Input(name: String) extends Vector {
    def forward(ctx: ForwardContext) = ???
  }
  case class Sigmoid(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(sigmoid(_))
  }
  case class Tanh(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(tanh(_))
  }

  // Trained vector or matrix of weights.
  trait Trained[T] {
    def name: String
    def random(size: Int)(implicit r: RandBasis): T
  }
  val initialRandomAmplitude = 0.01
  case class V(name: String) extends Trained[DoubleVector] {
    def random(size: Int)(implicit r: RandBasis) =
      randn(size) * initialRandomAmplitude
  }
  case class M(name: String) extends Trained[DoubleMatrix] {
    def random(size: Int)(implicit r: RandBasis) =
      randn((size, size)) * initialRandomAmplitude
  }

  private case class ForwardContext(
      network: Network,
      vertices: Seq[ID],
      edges: CompactUndirectedGraph,
      neighborState: GraphData,
      inputs: Map[String, GraphData]) {
    val vectorCache = collection.mutable.Map[String, GraphData]()
    val vectorsCache = collection.mutable.Map[String, GraphVectors]()
    def apply(v1: Vector, v2: Vector): Map[ID, (DoubleVector, DoubleVector)] = {
      val g1 = this(v1)
      val g2 = this(v2)
      vertices.map { id => id -> (g1(id), g2(id)) }.toMap
    }
    def apply(v: Vector): GraphData = {
      vectorCache.getOrElseUpdate(v.id, v match {
        case Input(name) => inputs(name)
        case v => v.forward(this)
      })
    }
    def apply(vs: Vectors): GraphVectors = {
      vectorsCache.getOrElseUpdate(vs.id, vs.forward(this))
    }
    def apply[T](t: Trained[T]) = network(t)
    def neighbors: GraphVectors = {
      vertices.map { id =>
        id -> edges.getNeighbors(id).map(neighborState(_))
      }.toMap
    }
  }

  object Network {
    // The public constructor does not set weights, so they get randomly initialized.
    def apply(size: Int, outputs: (String, Vector)*) = new Network(size, Map(), outputs.toMap)
  }
  case class Network private (
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
