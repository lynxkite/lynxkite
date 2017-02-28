package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import play.api.libs.json
import com.lynxanalytics.biggraph._

class WorkspaceTest extends FunSuite with graph_api.TestGraphOp {
  val controller = new BigGraphController(this)
  val ops = new frontend_operations.Operations(this)
  val user = serving.User.fake

  def create(name: String) =
    controller.createWorkspace(user, CreateWorkspaceRequest(name, "private"))
  def get(name: String): Workspace =
    controller.getWorkspace(user, GetWorkspaceRequest(name))
  def discard(name: String) =
    controller.discardEntry(user, DiscardEntryRequest(name))
  def using[T](name: String)(f: => T): T =
    try {
      create(name)
      f
    } finally discard(name)
  import WorkspaceJsonFormatters._
  import CheckpointRepository._
  def print[T: json.Writes](t: T): Unit = {
    println(json.Json.prettyPrint(json.Json.toJson(t)))
  }

  test("pagerank on example graph") {
    val eg = ops.getBoxMetadata("Example Graph").toBox("eg", Map(), 0, 0)
    val pr = ops.getBoxMetadata("PageRank").toBox("pr", Map(
      "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
      "iterations" -> "5", "direction" -> "all edges"), 0, 20)
    val ws = Workspace(List(eg, pr.connect("project", eg.output("project"))))
    val project = ws.state(user, ops, pr.output("project")).project
    import graph_api.Scripting._
    assert(project.vertexAttributes("pagerank").rdd.values.collect.toSet == Set(
      1.4099834026132592, 1.4099834026132592, 0.9892062327983842, 0.19082696197509774))
  }

  /*
  test("errors") {
    using("test-workspace") {
      val blank = get("test-workspace")
      val eg = AddBox(blank, "Example Graph", y = 20)
      val derive = AddBox(eg.ws, "Derived vertex attribute", y = 40, params = Map(
        "expr" -> "xxx", "type" -> "double", "output" -> "x"))
      val pr = AddBox(
        derive.ws, "PageRank", y = 60,
        params = Map(
          "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
          "iterations" -> "5", "direction" -> "all edges"))
      val connected1 = addArrow(pr.ws, eg.box.output("project"), derive.box.input("project"))
      val connected2 = addArrow(connected1, derive.box.output("project"), pr.box.input("project"))
      assertBoxesArrowsStates(connected2, 3, 2, 3)
      val ex = intercept[AssertionError] {
        connected2.stateMap(pr.box.output("project")).project
      }
      assert(ex.getMessage.contains("\"xxx\" is not defined"))
    }
  }
    */
}
