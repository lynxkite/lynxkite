// Collection of classes for building a neural network.
package com.lynxanalytics.biggraph.neural

import breeze.stats.distributions.RandBasis
import com.lynxanalytics.biggraph.graph_api._

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
  }

  trait Vector extends Gate[GraphData] {
    def *(m: M) = MultiplyWeights(this, m)
    def *(v: Vector) = MultiplyElements(this, v)
    def *(s: Double) = MultiplyScalar(this, s)
    def unary_- = this * (-1)
    def +(v: Vector) = AddElements(this, v)
    def -(v: Vector) = AddElements(this, -v)
    def +(v: V) = AddWeights(this, v)
  }

  trait Vectors extends Gate[GraphVectors] {
    def *(m: M) = MultiplyVectors(this, m)
  }

  case class MultiplyWeights(v: Vector, w: M) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(ctx(w) * _)
    def backward(ctx: BackwardContext) = ctx.add(this, v)(gradient => ctx(w).t * gradient)
  }
  case class MultiplyScalar(v: Vector, s: Double) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(s * _)
    def backward(ctx: BackwardContext) = ctx.add(this, v)(gradient => gradient / s)
  }
  case class MultiplyElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v1, v2).mapValues { case (v1, v2) => v1 :* v2 }
    def backward(ctx: BackwardContext) = {
      ctx.add(this, v1)(gradient => ctx(v2) :* gradient)
      ctx.add(this, v2)(gradient => ctx(v1) :* gradient)
    }
  }
  case class AddElements(v1: Vector, v2: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v1, v2).mapValues { case (v1, v2) => v1 + v2 }
    def backward(ctx: BackwardContext) = {
      ctx.add(this, v1)(identity)
      ctx.add(this, v2)(identity)
    }
  }
  case class AddWeights(v: Vector, w: V) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(ctx(w) + _)
    def backward(ctx: BackwardContext) = ctx.add(this, v)(identity)
  }
  case class MultiplyVectors(vs: Vectors, w: M) extends Vectors {
    def forward(ctx: ForwardContext) = ctx(vs).mapValues(_.map(ctx(w) * _))
    def backward(ctx: BackwardContext) = ctx.add(this, vs)(identity)
  }
  case class Sum(vs: Vectors) extends Vector {
    def forward(ctx: ForwardContext) = ctx(vs).mapValues { vectors =>
      val sum = DenseVector.zeros[Double](ctx.network.size)
      for (v <- vectors) sum += v
      sum
    }
    def backward(ctx: BackwardContext) = ctx.add(this, vs)(identity)
  }
  case class Neighbors() extends Vectors {
    def forward(ctx: ForwardContext) = ctx.neighbors
    def backward(ctx: BackwardContext) = ctx.addNeighbors(this)
  }
  case class Input(name: String) extends Vector {
    def forward(ctx: ForwardContext) = ctx.inputs(name)
    def backward(ctx: BackwardContext) = ctx.addInput(this)
  }
  case class Sigmoid(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(sigmoid(_))
    def backward(ctx: BackwardContext) = ctx.add(this, v) {
      gradient => ctx(this) :* (1.0 - ctx(this)) :* gradient
    }
  }
  case class Tanh(v: Vector) extends Vector {
    def forward(ctx: ForwardContext) = ctx(v).mapValues(tanh(_))
    def backward(ctx: BackwardContext) = ctx.add(this, v) {
      gradient => (1.0 - (ctx(this) :* ctx(this))) :* gradient
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
    val grads = gradients.toMap
    val ctx = BackwardContext(this, vertices, edges, values, grads)
    for (g <- grads.keys) {
      outputs(g).backward(ctx)
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

  def values(names: Map[String, Vector]) = GateValues(vectorCache, vectorsCache, names)
}

case class GateValues(
  vectorData: Iterable[(String, GraphData)],
  vectorsData: Iterable[(String, GraphVectors)],
  names: Map[String, Vector]) {
  val vectorMap = vectorData.toMap
  val vectorsMap = vectorsData.toMap
  def apply(v: Vector): GraphData = vectorMap(v.id)
  def apply(vs: Vectors): GraphData = vectorsMap(vs.id)
  def apply(name: String): GraphData = this(names(name))
}

private case class BackwardContext(
    network: Network,
    vertices: Seq[ID],
    edges: CompactUndirectedGraph,
    values: GateValues,
    gradients: Map[String, GraphData]) {
  val vectorGradients = collection.mutable.Map[String, GraphData]()
  val vectorsGradients = collection.mutable.Map[String, GraphVectors]()
  def apply(v: Vector): GraphData = values(v)
  def apply(vs: Vectors): GraphVectors = values(vs)
  def apply(m: M): DoubleMatrix = network(m)
  def apply(v: V): DoubleVector = network(v)(::, 0)

  // ctx.add(this, v)(gradient => ctx(w).t * gradient)
  def add(
}

trait NetworkGradients {
}
