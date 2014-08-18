package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class OperationsTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val ops = new Operations(this)
  val project = Project("project")
  def run(op: String, params: Map[String, String] = Map()) =
    ops.apply(ProjectOperationRequest("project", FEOperationSpec(op.replace(" ", "-"), params)))

  test("Derived vertex attribute") {
    run("Example Graph")
    run("Derived vertex attribute",
      Map("output" -> "output", "expr" -> "100 + age + 10 * name.length", "type" -> "Number"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 160.3, 1 -> 148.2, 2 -> 180.3, 3 -> 222.0))
  }
}
