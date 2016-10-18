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
      val keys = a.keys ++ b.keys
      keys.map { id =>
        id -> {
          (a.get(id), b.get(id)) match {
            case (Some(a), Some(b)) => adder.add(a, b)
            case (Some(a), None) => a
            case (None, Some(b)) => b
            case (None, None) => ???
          }
        }
      }.toMap
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
    //http://stackoverflow.com/questions/14882642/scala-why-mapvalues-produces-a-view-and-is-there-any-stable-alternatives
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
      ctx.add(v, gradient.mapValues(s * _))
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
      val netgrads = gradients.flatMap {
        case (id, gs) => gs.zip(vsd(id)).map { case (g, d) => g * d.t }
      }
      if (netgrads.nonEmpty) ctx.add(w, netgrads.reduce(_ + _))
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
  def apply(clipGradients: Boolean, size: Int, outputs: (String, Vector)*)(implicit r: RandBasis) =
    new Network(clipGradients, size, outputs.toMap)
}
case class Network private (
    clipGradients: Boolean,
    size: Int, outputs: Map[String, Vector],
    weights: Iterable[(String, DoubleMatrix)] = Iterable(),
    adagradMemory: Map[String, DoubleMatrix] = Map())(
        implicit r: RandBasis) {
  val allWeights = collection.mutable.Map(weights.toSeq: _*)
  def +(other: Network): Network = this.copy(
    weights = allWeights.map {
      case (name, value) => name -> (value + other.allWeights(name))
    },
    adagradMemory = adagradMemory.map {
      case (name, value) => name -> (value + other.adagradMemory(name))
    })
  def /(s: Double): Network = this.copy(
    weights = allWeights.mapValues(_ / s),
    adagradMemory = adagradMemory.mapValues(_ / s))

  def apply(t: Trained): DoubleMatrix = {
    allWeights.getOrElseUpdate(t.name, t.random(size))
  }

  def forward(
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
    neighbors: GraphData,
    inputs: (String, GraphData)*): GateValues = {
    val ctx = ForwardContext(this, vertices, edges, neighbors, inputs.toMap)
    for (o <- outputs.values) {
      ctx(o).view.force
    }
    ctx.values(outputs)
  }

  def backward(
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
    values: GateValues,
    gradients: (String, GraphData)*): NetworkGradients = {
    val ctx = BackwardContext(this, vertices, edges, values)
    for ((id, g) <- gradients) {
      outputs(id).backward(ctx, g)
    }
    ctx.gradients
  }

  def update(gradients: Iterable[NetworkGradients], learningRate: Double): (Network, Map[String, DoubleMatrix]) = {
    import breeze.linalg._
    import breeze.numerics._
    val sums = allWeights.keys.map {
      name => name -> gradients.map(_.trained(name)).reduce(_ + _)
    }.toMap
    if (clipGradients) {
      for ((k, v) <- sums) {
        clip.inPlace(v, -5.0, 5.0)
      }
    }
    val newAdagradMemory = sums.map {
      case (name, s) => adagradMemory.get(name) match {
        case Some(mem) => name -> (mem + (s :* s))
        case None => name -> (s :* s)
      }
    }
    val newWeights = allWeights.toMap.map {
      case (name, w) =>
        name -> (w - learningRate * sums(name) / sqrt(newAdagradMemory(name) + 1e-6))
    }
    (this.copy(weights = newWeights, adagradMemory = newAdagradMemory), sums)
  }

  def toDebugString = {
    allWeights.map { case (k, v) => s"$k: $v" }.mkString("\n")
  }
}

private case class ForwardContext(
    network: Network,
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
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
    import breeze.linalg._
    vertices.map { id =>
      if (edges(id) == List()) id -> List(0).map(_ => DenseVector.zeros[Double](network.size))
      else id -> edges(id).map(neighborState(_))
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
    edges: Map[ID, Seq[ID]],
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
    v.backward(this, gradient)
  }
  def add(vs: Vectors, gradients: GraphVectors): Unit = {
    vectorsGradients(vs.id) =
      vectorsGradients.get(vs.id).map(_ + gradients).getOrElse(gradients)
    vs.backward(this, gradients)
  }
  def add(m: M, gradient: DoubleMatrix): Unit = {
    trainedGradients(m.name) =
      trainedGradients.get(m.name).map(_ + gradient).getOrElse(gradient)
  }
  def add(v: V, gradient: DoubleVector): Unit = {
    val mgrad = gradient.asDenseMatrix.t
    trainedGradients(v.name) =
      trainedGradients.get(v.name).map(_ + mgrad).getOrElse(mgrad)
  }

  def addNeighbors(gradients: GraphVectors): Unit = {
    val ngrad: GraphData = gradients.toSeq.flatMap {
      case (id, gs) => edges(id).zip(gs)
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
    val neighbors: GraphData) {
  val vector = vectorGradients.toMap
  val vectors = vectorsGradients.toMap
  val trained = trainedGradients.toMap
  def apply(name: String): GraphData = vector(Gates.Input(name).id)
}
