package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object FakePull extends OpFromJson {
  class Input extends MagicInputSignature {
    // Assumed to be ExampleGraph
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val pull = edgeBundle(inputs.vs.entity, inputs.vs.entity, EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: play.api.libs.json.JsValue) = FakePull()
}
case class FakePull() extends TypedMetaGraphOp[FakePull.Input, FakePull.Output] {
  import FakePull._
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(
      o.pull,
      rc.sparkContext
        .parallelize(Seq((0L, Edge(0, 1)), (1L, Edge(1, 2)), (2L, Edge(2, 0)), (3L, Edge(3, 3))))
        .toSortedRDD(inputs.vs.rdd.partitioner.get))
  }
}

class PulledOverAttributeTest extends FunSuite with TestGraphOp {
  test("works with filters") {
    val g = ExampleGraph()().result

    val fop = VertexAttributeFilter(DoubleGT(10))
    val fopRes = fop(fop.attr, g.age).result

    val pop = PulledOverVertexAttribute[String]()
    val pulledAttr =
      pop(pop.function, fopRes.identity)(pop.originalAttr, g.name).result.pulledAttr

    assert(pulledAttr.rdd.collect.toMap == Map(0l -> "Adam", 1 -> "Eve", 2 -> "Bob"))
  }

  test("works with fake pull") {
    val g = ExampleGraph()().result

    val fop = FakePull()
    val fopRes = fop(fop.vs, g.vertices).result

    val pop = PulledOverVertexAttribute[String]()
    val pulledAttr = pop(pop.function, fopRes.pull)(pop.originalAttr, g.name).result.pulledAttr

    assert(pulledAttr.rdd.collect.toMap ==
      Map(0l -> "Eve", 1 -> "Bob", 2 -> "Adam", 3 -> "Isolated Joe"))
  }

  test("fails if bundle is not a partial function") {
    val g = ExampleGraph()().result
    val pop = PulledOverVertexAttribute[String]()
    intercept[AssertionError] {
      pop(pop.function, g.edges)
    }
  }
}
