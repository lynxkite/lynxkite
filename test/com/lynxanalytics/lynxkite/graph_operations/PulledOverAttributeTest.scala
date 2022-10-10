package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object FakePull extends OpFromJson {
  class Input extends MagicInputSignature {
    // Assumed to be ExampleGraph
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val pull = edgeBundle(inputs.vs.entity, inputs.vs.entity, EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: play.api.libs.json.JsValue) = FakePull()
}
case class FakePull() extends SparkOperation[FakePull.Input, FakePull.Output] {
  import FakePull._
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(
      o.pull,
      rc.sparkContext
        .parallelize(Seq((0L, Edge(1, 1)), (1L, Edge(2, 2)), (2L, Edge(3, 2))))
        .sortUnique(inputs.vs.rdd.partitioner.get))
  }
}

class PulledOverAttributeTest extends AnyFunSuite with TestGraphOp {
  test("works with filters") {
    val g = ExampleGraph()().result
    implicit val d = SerializableType.double
    val fop = VertexAttributeFilter(GT(10.0))
    val fopRes = fop(fop.attr, g.age).result

    val pop = PulledOverVertexAttribute[String]()
    val pulledAttr =
      pop(pop.function, fopRes.identity)(pop.originalAttr, g.name).result.pulledAttr

    assert(pulledAttr.rdd.collect.toMap == Map(0L -> "Adam", 1 -> "Eve", 2 -> "Bob"))
  }

  test("works with fake pull") {
    val g = ExampleGraph()().result

    val fop = FakePull()
    val fopRes = fop(fop.vs, g.vertices).result

    val namePop = PulledOverVertexAttribute[String]()
    val incomePop = PulledOverVertexAttribute[Double]()
    val pulledName = get(namePop(namePop.function, fopRes.pull)(namePop.originalAttr, g.name).result.pulledAttr)
    val pulledIncome =
      get(incomePop(incomePop.function, fopRes.pull)(incomePop.originalAttr, g.income).result.pulledAttr)

    assert(pulledName == Map(1 -> "Eve", 2 -> "Bob", 3 -> "Bob"))
    assert(pulledIncome == Map(2 -> 2000.0, 3 -> 2000.0))
  }

  test("fails if bundle is not a partial function") {
    val g = ExampleGraph()().result
    val pop = PulledOverVertexAttribute[String]()
    intercept[AssertionError] {
      pop(pop.function, g.edges)
    }
  }
}
