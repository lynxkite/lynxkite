// Collection of classes for building a neural network.
package com.lynxanalytics.biggraph.neural

import breeze.stats.distributions.RandBasis
import com.lynxanalytics.biggraph.graph_api._
import org.apache.commons.math3.random.MersenneTwister

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
  implicit object VectorsGateAdder extends Adder[DoubleVector] {
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

  /* You can define neural network layouts by using small building blocks called
  gates. The input nodes of the network are gates. The other gates apply a
  function to the outputs of some previous gates.
  Gates perform the corresponding operation in every vertex of the graph parallel.
  */
  trait Gate[Output] extends Product {
    // Plain toString on case classes is enough to uniquely identify vectors.
    private[neural] def id = this.toString
    private[neural] def forward(fm: ForwardMemory): Output
    private[neural] def backward(bm: BackwardMemory, gradient: Output): Unit
  }
  trait VectorGate extends Gate[VectorGraph] {
    // Translate operations to gates.
    def *(v: VectorGate) = VectorVectorMultiplication(this, v)
    def *(s: Double) = VectorScalarMultiplication(this, s)
    def unary_- = this * (-1)
    def +(v: VectorGate) = VectorVectorAddition(this, v)
    def -(v: VectorGate) = VectorVectorAddition(this, -v)
    def +(v: BiasVector) = VectorBiasAddition(this, v)
  }
  trait VectorsGate extends Gate[VectorsGraph] {
    def *(m: WeightMatrix) = MatrixVectorsMultiplication(this, m)
  }

  //Trainable objects are modified during the training.
  trait Trainable {
    def name: String
    def random(size: Int)(implicit r: RandBasis): DoubleMatrix
  }
  val initialRandomAmplitude = 0.01
  case class BiasVector(name: String) extends Trainable {
    def random(size: Int)(implicit r: RandBasis) =
      randn((size, 1)) * initialRandomAmplitude
  }
  case class WeightMatrix(name: String) extends Trainable {
    def *(v: VectorGate) = MatrixVectorMultiplication(this, v)
    def random(size: Int)(implicit r: RandBasis) =
      randn((size, size)) * initialRandomAmplitude
  }

  case class MatrixVectorMultiplication(w: WeightMatrix, v: VectorGate) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(v).mapValues(fm(w) * _)
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {
      val wt = bm(w).t
      bm.add(v, gradient.mapValues(g => wt * g))
      val vd = bm(v)
      bm.add(w, gradient.map { case (id, g) => g * vd(id).t }.reduce(_ + _))
    }
  }
  case class VectorScalarMultiplication(v: VectorGate, s: Double) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(v).mapValues(s * _)
    def backward(bm: BackwardMemory, gradient: VectorGraph) =
      bm.add(v, gradient.mapValues(s * _))
  }
  case class VectorVectorMultiplication(v1: VectorGate, v2: VectorGate) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(v1, v2).mapValues { case (v1, v2) => v1 :* v2 }
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {
      val v1d = bm(v1)
      val v2d = bm(v2)
      bm.add(v1, gradient.map { case (id, g) => id -> (v2d(id) :* g) })
      bm.add(v2, gradient.map { case (id, g) => id -> (v1d(id) :* g) })
    }
  }
  case class VectorVectorAddition(v1: VectorGate, v2: VectorGate) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(v1, v2).mapValues { case (v1, v2) => v1 + v2 }
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {
      bm.add(v1, gradient)
      bm.add(v2, gradient)
    }
  }
  case class VectorBiasAddition(v: VectorGate, w: BiasVector) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(v).mapValues(fm(w) + _)
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {
      bm.add(v, gradient)
      bm.add(w, gradient.values.reduce(_ + _))
    }
  }
  case class MatrixVectorsMultiplication(vs: VectorsGate, w: WeightMatrix) extends VectorsGate {
    def forward(fm: ForwardMemory) = fm.activation(vs).mapValues(_.map(fm(w) * _))
    def backward(bm: BackwardMemory, gradients: VectorsGraph) = {
      val wt = bm(w).t
      bm.add(vs, gradients.mapValues(_.map(g => wt * g)))
      val vsd = bm(vs)
      val netgrads = gradients.flatMap {
        case (id, gs) => gs.zip(vsd(id)).map { case (g, d) => g * d.t }
      }
      if (netgrads.nonEmpty) bm.add(w, netgrads.reduce(_ + _))
    }
  }
  case class VectorsAddition(vs: VectorsGate) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(vs).mapValues { vectors =>
      val sum = DenseVector.zeros[Double](fm.network.size)
      for (v <- vectors) sum += v
      sum
    }
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {
      val vsd = bm(vs)
      bm.add(vs, gradient.map { case (id, g) => id -> bm(vs)(id).map(_ => g) })
    }
  }

  case class NeighborsVectorCollection(v: VectorGate) extends VectorsGate {
    def forward(fm: ForwardMemory) = {
      import breeze.linalg._
      fm.vertices.map { id =>
        if (fm.edges(id) == List()) id -> List(0).
          map(_ => DenseVector.zeros[Double](fm.network.size))
        else id -> fm.edges(id).map(fm.activation(v)(_))
      }.toMap
    }
    def backward(bm: BackwardMemory, gradients: VectorsGraph) = {
      val ngrad: VectorGraph = gradients.toSeq.flatMap {
        case (id, gs) => bm.edges(id).zip(gs)
      }.groupBy(_._1).mapValues(_.map(_._2).reduce(_ + _))
      if (ngrad.nonEmpty) bm.add(v, ngrad)
    }
  }
  case class Input(name: String) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.inputs(name)
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {}
  }
  case class Sigmoid(v: VectorGate) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(v).mapValues(sigmoid(_))
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {
      val d = bm(this)
      bm.add(v, gradient.map { case (id, g) => id -> (d(id) :* (1.0 - d(id)) :* g) })
    }
  }
  case class Tanh(v: VectorGate) extends VectorGate {
    def forward(fm: ForwardMemory) = fm.activation(v).mapValues(tanh(_))
    def backward(bm: BackwardMemory, gradient: VectorGraph) = {
      val d = bm(this)
      bm.add(v, gradient.map { case (id, g) => id -> ((1.0 - (d(id) :* d(id))) :* g) })
    }
  }
}
import Gates._

object Network {
  // The public constructor does not set weights and biases, so they get randomly initialized.
  def apply(clipGradients: Boolean, size: Int, output: VectorGate)(implicit r: RandBasis) =
    new Network(clipGradients, size, output)
}
case class Network private (
    clipGradients: Boolean,
    size: Int, output: VectorGate,
    trainables: Iterable[(String, DoubleMatrix)] = Iterable(),
    adagradMemory: Map[String, DoubleMatrix] = Map())(
        implicit r: RandBasis) {
  val expandableTrainables = collection.mutable.Map(trainables.toSeq: _*)
  def +(other: Network): Network = this.copy(
    trainables = expandableTrainables.map {
      case (name, value) => name -> (value + other.expandableTrainables(name))
    },
    adagradMemory = adagradMemory.map {
      case (name, value) => name -> (value + other.adagradMemory(name))
    })
  def /(s: Double): Network = this.copy(
    trainables = expandableTrainables.mapValues(_ / s),
    adagradMemory = adagradMemory.mapValues(_ / s))

  def apply(t: Trainable): DoubleMatrix = {
    expandableTrainables.getOrElseUpdate(t.name, t.random(size))
  }

  def forward(
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
    inputs: (String, GraphData)*): GateValues = {
    val ctx = ForwardContext(this, vertices, edges, inputs.toMap)
    for (o <- outputs.values) {
      ctx(o).view.force
    }
    ctx.values(outputs)
  }

  def backward(
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
    values: GateValues,
    gradients: (String, VectorGraph)*: NetworkGradients = {
    val bm = BackwardMemory(this, vertices, edges, values)
    for((id,g) <- gradients) {
    	outputs(id).backward(bm, g)
    bm.gradients
  }

  def update(gradients: NetworkGradients, learningRate: Double): (Network, Map[String, DoubleMatrix]) = {
    import breeze.linalg._
    import breeze.numerics._
    val sums = expandableTrainables.keys.map {
      name => name -> gradients.trained(name)
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
    val newTrainables = expandableTrainables.toMap.map {
      case (name, w) =>
        name -> (w - learningRate * sums(name) / sqrt(newAdagradMemory(name) + 1e-6))
    }
    (this.copy(trainables = newTrainables, adagradMemory = newAdagradMemory), sums)
  }

  def toDebugString = {
    expandableTrainables.map { case (k, v) => s"$k: $v" }.mkString("\n")
  }
}

private case class ForwardMemory(
    network: Network,
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
    inputs: Map[String, VectorGraph]) {
  val vectorCache = collection.mutable.Map[String, VectorGraph]()
  val vectorsCache = collection.mutable.Map[String, VectorsGraph]()

  def activation(v1: VectorGate, v2: VectorGate): Map[ID, (DoubleVector, DoubleVector)] = {
    val g1 = this.activation(v1)
    val g2 = this.activation(v2)
    vertices.map { id => id -> (g1(id), g2(id)) }.toMap
  }
  def activation(v: VectorGate): VectorGraph = {
    vectorCache.getOrElseUpdate(v.id, v.forward(this))
  }
  def activation(vs: VectorsGate): VectorsGraph = {
    vectorsCache.getOrElseUpdate(vs.id, vs.forward(this))
  }
  def apply(m: WeightMatrix): DoubleMatrix = network(m)
  def apply(v: BiasVector): DoubleVector = network(v)(::, 0)
  def values(name: VectorGate) = new GateValues(vectorCache, vectorsCache, name)
}

class GateValues(
    vectorData: Iterable[(String, VectorGraph)],
    vectorsData: Iterable[(String, VectorsGraph)],
    names: Map[String,VectorGate]) {
  val vectorMap = vectorData.toMap
  val vectorsMap = vectorsData.toMap
  def apply(v: VectorGate): VectorGraph = vectorMap(v.id)
  def apply(vs: VectorsGate): VectorsGraph = vectorsMap(vs.id)
  def apply(name: VectorGate): VectorGraph = this(names(name))
}

private case class BackwardMemory(
    network: Network,
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
    values: GateValues) {
  val vectorGradients = collection.mutable.Map[String, VectorGraph]()
  val vectorsGradients = collection.mutable.Map[String, VectorsGraph]()
  val trainedGradients = collection.mutable.Map[String, DoubleMatrix]()
  var neighborGradients: VectorGraph = null
  def apply(v: VectorGate): VectorGraph = values(v)
  def apply(vs: VectorsGate): VectorsGraph = values(vs)
  def apply(m: WeightMatrix): DoubleMatrix = network(m)
  def apply(v: BiasVector): DoubleVector = network(v)(::, 0)

  def add(v: VectorGate, gradient: VectorGraph): Unit = {
    vectorGradients(v.id) =
      vectorGradients.get(v.id).map(_ + gradient).getOrElse(gradient)
    v.backward(this, gradient)
  }
  def add(vs: VectorsGate, gradients: VectorsGraph): Unit = {
    vectorsGradients(vs.id) =
      vectorsGradients.get(vs.id).map(_ + gradients).getOrElse(gradients)
    vs.backward(this, gradients)
  }
  def add(m: WeightMatrix, gradient: DoubleMatrix): Unit = {
    trainedGradients(m.name) =
      trainedGradients.get(m.name).map(_ + gradient).getOrElse(gradient)
  }
  def add(v: BiasVector, gradient: DoubleVector): Unit = {
    val mgrad = gradient.asDenseMatrix.t
    trainedGradients(v.name) =
      trainedGradients.get(v.name).map(_ + mgrad).getOrElse(mgrad)
  }

  def gradients = new NetworkGradients(
    vectorGradients, vectorsGradients, trainedGradients, neighborGradients)
}

class NetworkGradients(
    vectorGradients: Iterable[(String, VectorGraph)],
    vectorsGradients: Iterable[(String, VectorsGraph)],
    trainedGradients: Iterable[(String, DoubleMatrix)],
    val neighbors: VectorGraph) {
  val vector = vectorGradients.toMap
  val vectors = vectorsGradients.toMap
  val trained = trainedGradients.toMap
  def apply(name: String): VectorGraph = vector(Gates.Input(name).id)
}

abstract class Layout(networkSize: Int, gradientCheckOn: Boolean, radius: Int)(implicit rnd: RandBasis) {
  def ownStateInputs: Seq[String]
  def neighborsStateInputs: Seq[String]
  def connection = ownStateInputs ++ neighborsStateInputs
  def outputGate: String
  def block(input: Map[String, VectorGate], round: Int): Map[String, VectorGate]
  def getNetwork = {
    val finalState = {
      (0 until radius).foldLeft {
        connection.map(name => {
          val input: VectorGate = Input(name)
          (name, input)
        }).toMap
      } { case (a, b) => block(a, b) }(outputGate)
    }
    Network(clipGradients = !gradientCheckOn,
      size = networkSize,
      "final state" -> finalState
    )
  }
}
class GRU(networkSize: Int, gradientCheckOn: Boolean, radius: Int)(implicit rnd: RandBasis) extends Layout(networkSize, gradientCheckOn, radius) {
  def ownStateInputs = Seq("own state")
  def neighborsStateInputs = Seq("neighbors state")
  def outputGate = "own state"
  def block(fromPrevious: Map[String, VectorGate], round: Int) = {
    val vs = NeighborsVectorCollection(fromPrevious("neighbors state"))
    val state = fromPrevious("own state")
    val input = WeightMatrix("edge matrix") * VectorsAddition(vs) + BiasVector("edge bias")
    val update = Sigmoid(WeightMatrix("update i") * input + WeightMatrix("update h") * state)
    val reset = Sigmoid(WeightMatrix("reset i") * input + WeightMatrix("reset h") * state)
    val tilde = Tanh(WeightMatrix("activation i") * input + WeightMatrix("activation h") * (reset * state))
    val newState = state - update * state + update * tilde
    Map("own state" -> newState, "neighbors state" -> newState)
  }
}
class LSTM(networkSize: Int, gradientCheckOn: Boolean, radius: Int)(implicit rnd: RandBasis) extends Layout(networkSize, gradientCheckOn, radius) {
  def ownStateInputs = Seq("own cell", "own hidden")
  def neighborsStateInputs = Seq("neighbors cell")
  def outputGate = "own hidden"
  def block(fromPrevious: Map[String, VectorGate], round: Int) = {
    val vs = NeighborsVectorCollection(fromPrevious("neighbors cell"))
    val cell = fromPrevious("own cell")
    val hidden = fromPrevious("own hidden")
    val input = WeightMatrix("edge matrix") * VectorsAddition(vs) + BiasVector("edge bias")
    val forget = Sigmoid(WeightMatrix("forget h") * hidden + WeightMatrix("forget i") * input + BiasVector("forget b"))
    val chooseUpdate = Sigmoid(WeightMatrix("choose update h") * hidden + WeightMatrix("choose update i") * input + BiasVector("choose update b"))
    val tilde = Tanh(WeightMatrix("tilde h") * hidden + WeightMatrix("tilde i") * input + BiasVector("tilde b"))
    val newCell = cell * forget + chooseUpdate * tilde
    val chooseOutput = Sigmoid(WeightMatrix("choose output h") * hidden + WeightMatrix("choose output i") * input + BiasVector("choose output b"))
    val newHidden = chooseOutput * Tanh(newCell)
    Map("own cell" -> newCell, "own hidden" -> newHidden, "neighbors cell" -> newCell)
  }
}
class MLP(networkSize: Int, gradientCheckOn: Boolean, radius: Int)(implicit rnd: RandBasis) extends Layout(networkSize, gradientCheckOn, radius) {
  def ownStateInputs = Seq("own state")
  def neighborsStateInputs = Seq("neighbors state")
  def outputGate = "own state"
  def block(fromPrevious: Map[String, VectorGate], round: Int) = {
    val vs = NeighborsVectorCollection(fromPrevious("neighbors state"))
    val input = WeightMatrix(s"edge matrix ${round}") * VectorsAddition(vs) + BiasVector(s"edge bias ${round}")
    val state = WeightMatrix(s"state matrix ${round}") * fromPrevious("own state") + BiasVector(s"state bias ${round}")
    val newState = Tanh(input + state)
    Map("own state" -> newState, "neighbors state" -> newState)
  }
}
