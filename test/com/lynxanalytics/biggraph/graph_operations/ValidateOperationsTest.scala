package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

object ValidateOperationsTest {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val es1 = edgeBundle(vs, vs)
    val es2 = edgeBundle(vs, vs, idSet = vs)
    val vertexAttr = vertexAttribute[Double](vs)
    val edgeAttr = edgeAttribute[Double](es1)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
    val es1 = edgeBundle(vs, vs)
    val es2 = edgeBundle(vs, vs, idSet = vs)
    val vertexAttr = vertexAttribute[Double](vs)
    val edgeAttr = edgeAttribute[Double](es1)
  }
  object Source extends OpFromJson {
    def fromJson(j: JsValue) = Source((j \ "seed").as[Int])
  }
  case class Source(seed: Int) extends SparkOperation[NoInput, Output] {
    @transient override lazy val inputs = new NoInput
    def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
    override def toJson = Json.obj("seed" -> seed)
    def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = ???
  }
  object TestOperation extends OpFromJson {
    def fromJson(j: JsValue) = TestOperation()
  }
  case class TestOperation() extends SparkOperation[Input, Output] {
    @transient override lazy val inputs = new Input
    def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
    def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = ???
  }
}

import ValidateOperationsTest._
class ValidateOperationsTest extends AnyFunSuite with TestGraphOp {
  val s1 = Source(1).result
  val s2 = Source(2).result
  val op = TestOperation()

  test("all good") {
    op(op.es1, s1.es1)(op.es2, s1.es2)(
      op.vertexAttr, s1.vertexAttr)(op.edgeAttr, s1.edgeAttr).result
  }
  test("edgeAttr is missing") {
    val e = intercept[java.util.NoSuchElementException] {
      op(op.es1, s1.es1)(op.es2, s1.es2)(
        op.vertexAttr, s1.vertexAttr).result
    }
    assert(e.getMessage.contains("key not found: 'edgeAttr"), e)
  }
  test("vertexAttr is for a different vertex set") {
    val e = intercept[java.lang.AssertionError] {
      op(op.es1, s1.es1)(op.vertexAttr, s2.vertexAttr)
    }
    assert(e.getMessage.contains("Collision: ArrayBuffer('vs)"), e)
  }
  test("edgeAttr is for a different edge bundle") {
    val e = intercept[java.lang.AssertionError] {
      op(op.es1, s1.es1)(op.edgeAttr, s2.edgeAttr)
    }
    assert(e.getMessage.matches(
      raw".*'edgeAttr = .* \(edgeAttr of .* \(Source\(2\)\)\) is for" +
        raw" .* \(es1-idSet of .* \(Source\(2\)\)\), not for .* \(es1-idSet of .* \(Source\(1\)\)\)"), e)
  }
  test("edge attribute set before edge bundle") {
    val e = intercept[java.lang.AssertionError] {
      op(op.edgeAttr, s2.edgeAttr)(op.es1, s1.es1)
    }
    assert(e.getMessage.contains(
      "The edge bundle input ('es1) has to be provided before the attribute ('edgeAttr)"), e)
  }
  test("src & dst are good, idSet is bad") {
    op(op.es1, s1.es2) // No idSet requirement, substitute is accepted.
    val e = intercept[java.lang.AssertionError] {
      op(op.es2, s1.es1) // idSet requirement is not met.
    }
    assert(e.getMessage.contains("Collision: ArrayBuffer('vs)"), e)
  }
}
