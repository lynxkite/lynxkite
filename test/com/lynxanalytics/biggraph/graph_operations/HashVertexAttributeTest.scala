package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class HashVertexAttributeTest extends FunSuite with TestGraphOp {
  val secret = "Dennis Bergkamp"
  test("example graph") {
    val eg = ExampleGraph()().result
    val op = HashVertexAttribute(HashVertexAttribute.protectFromLogging(secret))
    val res = op(op.vs, eg.vertices)(op.attr, eg.name).result.hashed
    val hash = res.rdd.collect.toSeq.sorted
    assert(hash == Seq(0 -> "2403EB1237F35C2B69D6B066", 1 -> "2FC9CC49992F819425E20E98",
      2 -> "11D5F0C757072A18789617AC", 3 -> "F0F4DA7D574C61036D8C1B93"))
  }
  test("Log protection is enforced") {
    val e = intercept[Throwable] {
      HashVertexAttribute(secret)
    }
    assert(!e.getMessage.contains(secret))
  }
  test("Secret is checked for closing bracket") {
    val protectedSecret = HashVertexAttribute.protectFromLogging("Dennis)Bergkamp")
    val e = intercept[Throwable] {
      HashVertexAttribute(protectedSecret)
    }
    assert(!e.getMessage.contains("Dennis") && !e.getMessage.contains("Bergkamp"))
  }
  test("getContents works") {
    val masked = HashVertexAttribute.protectFromLogging(secret)
    val orig = HashVertexAttribute.getContents(masked)
    assert(secret == orig)
  }
}
