// Collection of classes for building a neural network.
package com.lynxanalytics.biggraph.neural

import breeze.stats.distributions.RandBasis
import com.lynxanalytics.biggraph.graph_api._

trait Adder[T] {
  def add(a: T, b: T): T
}

object Implicits {
  implicit class AdderOps[T: Adder](self: T) {
    def +(other: T): T = {
      val adder = implicitly[Adder[T]]
      adder.add(self, other)
    }
  }
  implicit object VectorAdder extends Adder[DoubleVector] {
    def add(a: DoubleVector, b: DoubleVector) = a + b
  }
  implicit object VectorsAdder extends Adder[Iterable[DoubleVector]] {
    def add(a: Iterable[DoubleVector], b: Iterable[DoubleVector]) =
      a.zip(b).map { case (a, b) => a + b }
  }
  class GraphAdder[T](adder: Adder[T]) extends Adder[Graph[T]] {
    def add(a: Graph[T], b: Graph[T]): Graph[T] = {
      a.map { case (id, v) => id -> adder.add(v, b(id)) }
    }
  }
  // For some reason making GraphAdder implicit did not achieve the same thing.
  implicit def graphAdder[T](implicit self: Adder[T]): Adder[Graph[T]] =
    new GraphAdder(self)
}
import Implicits._

object Gates {
  import breeze.linalg._
  import breeze.numerics._

  trait Gate[Output] extends Product {
    // Finds all instances of a type in the network leading up to this node.
    protected def find[T: reflect.ClassTag]: Seq[T] = {
      this.productIterator.flatMap {
        case x if (reflect.classTag[T].runtimeClass.isInstance(x)) => Some(x.asInstanceOf[T])
        case x: Gate[_] => x.find[T]
        case _ => None
      }.toSeq
    }

    // Plain toString on case classes is enough to uniquely identify vectors.
    private[neural] def id = this.toString
    private[neural] def forward(ctx: ForwardContext): Output
    private[neural] def backward(ctx: BackwardContext, gradient: Output): Unit
  }

  trait Vector extends Gate[GraphData] {
    // Translate operations to gates.
    def *(m: M) = MatrixVector(this, m)
    def *(v: Vector) = MultiplyElements(this, v)
    def *(s: Double) = MultiplyScalar(this, s)
    def unary_- = this * (-1)
    def +(v: Vector) = AddElements(this, v)
    def -(v: Vector) = AddElements(this, -v)
    def +(v: V) = AddWeights(this, v)

    // Utilities for applying simple functions to GraphData.
    def perVertex(fn: DoubleVector => DoubleVector): GraphData => GraphData =
      _.mapValues(fn)
    def withData[T](data: GraphData)(
      fn: (DoubleVector, DoubleVector) => T): GraphData => Graph[T] = {
      _.map { case (id, v) => id -> fn(v, data(id)) }
    }
  }

  trait Vectors extends Gate[GraphVectors] {
    def *(m: M) = MatrixVectors(this, m)

    // Utility for applying simple functions to GraphVectors.
    def perVertex(fn: DoubleVector => DoubleVector): GraphVectors => GraphVectors =
      _.mapValues(_.map(fn))
  }

  case class MatrixVector(v: Vector, w: M) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(ctx(w) * _)
    def backward(ctx: BackwardContext, gradient: GraphData) = {
      val wt = ctx(w).t
      ctx.add(v, gradient.mapValues(g => wt * g))
      val vd = ctx(v)
      ctx.add(w, gradient.map { case (id, g) => g * vd(id).t }.reduce(_ + _))
    }
  }
  case class MultiplyScalar(v: Vector, s: Double) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(s * _)
    def backward(ctx: BackwardContext, gradient: GraphData) =
      ctx.add(v, gradient.mapValues(g => g / s))
  }
  case class MultiplyElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v1, v2).mapValues { case (v1, v2) => v1 :* v2 }
    def backward(ctx: BackwardContext, gradient: GraphData) = {
      val v1d = ctx(v1)
      val v2d = ctx(v2)
      ctx.add(v1, gradient.map { case (id, g) => id -> (v2d(id) :* g) })
      ctx.add(v2, gradient.map { case (id, g) => id -> (v1d(id) :* g) })
    }
  }
  case class AddElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v1, v2).mapValues { case (v1, v2) => v1 + v2 }
    def backward(ctx: BackwardContext, gradient: GraphData) = {
      ctx.add(v1, gradient)
      ctx.add(v2, gradient)
    }
  }
  case class AddWeights(v: Vector, w: V) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(ctx(w) + _)
    def backward(ctx: BackwardContext, gradient: GraphData) = {
      ctx.add(v, gradient)
      ctx.add(w, gradient.values.reduce(_ + _))
    }
  }
  case class MatrixVectors(vs: Vectors, w: M) extends Vectors {
    def forward(ctx: ForwardContext) = ctx(vs).mapValues(_.map(ctx(w) * _))
    def backward(ctx: BackwardContext, gradients: GraphVectors) = {
      val wt = ctx(w).t
      ctx.add(vs, gradients.mapValues(_.map(g => wt * g)))
      val vsd = ctx(vs)
      ctx.add(w, gradients.flatMap {
        case (id, gs) => gs.zip(vsd(id)).map { case (g, d) => g * d.t }
      }.reduce(_ + _))
    }
  }
  case class Sum(vs: Vectors) extends Vector {
    def forward(ctx: ForwardContext) = ctx(vs).mapValues { vectors =>
      val sum = DenseVector.zeros[Double](ctx.network.size)
      for (v <- vectors) sum += v
      sum
    }
    def backward(ctx: BackwardContext, gradient: GraphData) = {
      val vsd = ctx(vs) // We need this to know the cardinality of vs.
      ctx.add(vs, gradient.map { case (id, g) => id -> vsd(id).map(_ => g) })
    }
  }
  case class Neighbors() extends Vectors {
    def forward(ctx: ForwardContext) = ctx.neighbors
    def backward(ctx: BackwardContext, gradients: GraphVectors) = ctx.addNeighbors(gradients)
  }
  case class Input(name: String) extends Vector {
    def forward(ctx: ForwardContext) = ctx.inputs(name)
    def backward(ctx: BackwardContext, gradient: GraphData) = {}
  }
  case class Sigmoid(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(sigmoid(_))
    def backward(ctx: BackwardContext, gradient: GraphData) = {
      val d = ctx(this)
      ctx.add(v, gradient.map { case (id, g) => id -> (d(id) :* (1.0 - d(id)) :* g) })
    }
  }
  case class Tanh(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(tanh(_))
    def backward(ctx: BackwardContext, gradient: GraphData) = {
      val d = ctx(this)
      ctx.add(v, gradient.map { case (id, g) => id -> ((1.0 - (d(id) :* d(id))) :* g) })
    }
  }

  // Trained matrix of weights.
  trait Trained {
    def name: String
    def random(size: Int)(implicit r: RandBasis): DoubleMatrix
  }
  val initialRandomAmplitude = 0.01
  case class V(name: String) extends Trained {
    def random(size: Int)(implicit r: RandBasis) =
      randn((size, 1)) * initialRandomAmplitude
  }
  case class M(name: String) extends Trained {
    def random(size: Int)(implicit r: RandBasis) =
      randn((size, size)) * initialRandomAmplitude
  }
}
import Gates._

object Network {
  // The public constructor does not set weights, so they get randomly initialized.
  def apply(size: Int, outputs: (String, Vector)*) = new Network(size, Map(), outputs.toMap)
}
case class Network private (
    size: Int, weights: Map[String, DoubleMatrix], outputs: Map[String, Vector]) {
  implicit val randBasis = RandBasis.mt0
  val allWeights = collection.mutable.Map(weights.toSeq: _*)
  def apply(t: Trained): DoubleMatrix = allWeights.getOrElseUpdate(t.name, t.random(size))

  def forward(
    vertices: Seq[ID],
    edges: CompactUndirectedGraph,
    neighbors: GraphData,
    inputs: (String, GraphData)*): GateValues = {
    val ctx = ForwardContext(this, vertices, edges, neighbors, inputs.toMap)
    for (o <- outputs.values) {
      ctx(o)
    }
    ctx.values(outputs)
  }

  def backward(
    vertices: Seq[ID],
    edges: CompactUndirectedGraph,
    values: GateValues,
    gradients: (String, GraphData)*): NetworkGradients = {
    val ctx = BackwardContext(this, vertices, edges, values)
    for ((id, g) <- gradients) {
      outputs(id).backward(ctx, g)
    }
    ctx.gradients
  }

  def update(gradients: Iterable[NetworkGradients]): Network = ???
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
    vectorCache.getOrElseUpdate(v.id, v.forward(this))
  }
  def apply(vs: Vectors): GraphVectors = {
    vectorsCache.getOrElseUpdate(vs.id, vs.forward(this))
  }
  def apply(m: M): DoubleMatrix = network(m)
  def apply(v: V): DoubleVector = network(v)(::, 0)
  def neighbors: GraphVectors = {
    vertices.map { id =>
      id -> edges.getNeighbors(id).map(neighborState(_))
    }.toMap
  }

  def values(names: Map[String, Vector]) = new GateValues(vectorCache, vectorsCache, names)
}

class GateValues(
    vectorData: Iterable[(String, GraphData)],
    vectorsData: Iterable[(String, GraphVectors)],
    names: Map[String, Vector]) {
  val vectorMap = vectorData.toMap
  val vectorsMap = vectorsData.toMap
  def apply(v: Vector): GraphData = vectorMap(v.id)
  def apply(vs: Vectors): GraphVectors = vectorsMap(vs.id)
  def apply(name: String): GraphData = this(names(name))
}

private case class BackwardContext(
    network: Network,
    vertices: Seq[ID],
    edges: CompactUndirectedGraph,
    values: GateValues) {
  val vectorGradients = collection.mutable.Map[String, GraphData]()
  val vectorsGradients = collection.mutable.Map[String, GraphVectors]()
  val trainedGradients = collection.mutable.Map[String, DoubleMatrix]()
  var neighborGradients: GraphData = null
  def apply(v: Vector): GraphData = values(v)
  def apply(vs: Vectors): GraphVectors = values(vs)
  def apply(m: M): DoubleMatrix = network(m)
  def apply(v: V): DoubleVector = network(v)(::, 0)

  def add(v: Vector, gradient: GraphData): Unit = {
    vectorGradients(v.id) =
      vectorGradients.get(v.id).map(_ + gradient).getOrElse(gradient)
  }
  def add(vs: Vectors, gradients: GraphVectors): Unit = {
    vectorsGradients(vs.id) =
      vectorsGradients.get(vs.id).map(_ + gradients).getOrElse(gradients)
  }
  def add(m: M, gradient: DoubleMatrix): Unit = {
    trainedGradients(m.name) =
      trainedGradients.get(m.name).map(_ + gradient).getOrElse(gradient)
  }
  def add(v: V, gradient: DoubleVector): Unit = {
    val mgrad = gradient.asDenseMatrix
    trainedGradients(v.name) =
      trainedGradients.get(v.name).map(_ + mgrad).getOrElse(mgrad)
  }

  def addNeighbors(gradients: GraphVectors): Unit = {
    val ngrad: GraphData = gradients.flatMap {
      case (id, gs) => edges.getNeighbors(id).zip(gs)
    }.groupBy(_._1).mapValues(_.map(_._2).reduce(_ + _))
    if (neighborGradients == null) neighborGradients = ngrad
    else neighborGradients = neighborGradients + ngrad
  }

  def gradients = new NetworkGradients(
    vectorGradients, vectorsGradients, trainedGradients, neighborGradients)
}

class NetworkGradients(
    vectorGradients: Iterable[(String, GraphData)],
    vectorsGradients: Iterable[(String, GraphVectors)],
    trainedGradients: Iterable[(String, DoubleMatrix)],
    neighborGradients: GraphData) {
  val inputMap = vectorGradients.toMap
  def apply(name: String): GraphData = inputMap(Gates.Input(name).id)
  def neighbors = neighborGradients
}
