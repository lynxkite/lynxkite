package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class HashVertexAttributeTest extends FunSuite with TestGraphOp {
  val secret = "Dennis Bergkamp"
  test("example graph") {
    val eg = ExampleGraph()().result
    val op = HashVertexAttribute(HashVertexAttribute.makeSecret(secret))
    val res = op(op.vs, eg.vertices)(op.attr, eg.name).result.hashed
    val hash = res.rdd.collect.toSeq.sorted
    assert(hash == Seq(
      0 -> "5B2556386BAB0C7DE25F2AC26A8B68AF6B9E2B2E1DB71E613D3FCDCB4F5AEC6E",
      1 -> "BF529EB7F98870D83E852883CEC00BA21E1E8F73E5742224C2D65551CB893006",
      2 -> "88CE60723DC05F68541966476BA770D35D714FC01B9F89A582A2B25B0273D406",
      3 -> "6AB6058AEAD44730FB1572FDB51AE81B1329F0DD35218C6BC661EBD3C3A3913A"))
  }
  test("Log protection is enforced") {
    val e = intercept[Throwable] {
      HashVertexAttribute(secret)
    }
    assert(!e.getMessage.contains(secret))
  }
  test("Secret is checked for closing bracket in makeSecret") {
    val e = intercept[Throwable] {
      HashVertexAttribute.makeSecret("Dennis)Bergkamp")
    }
    assert(!e.getMessage.contains("Dennis") && !e.getMessage.contains("Bergkamp"))
  }
  test("Secret is checked for closing bracket in HashVertexAttribute case class") {
    val e = intercept[Throwable] {
      HashVertexAttribute("Dennis)Bergkamp")
    }
    assert(!e.getMessage.contains("Dennis") && !e.getMessage.contains("Bergkamp"))
  }
  test("getContents works") {
    val protectedSecret = HashVertexAttribute.makeSecret(secret)
    val orig = HashVertexAttribute.getSecret(protectedSecret)
    assert(secret == orig)
  }
}
