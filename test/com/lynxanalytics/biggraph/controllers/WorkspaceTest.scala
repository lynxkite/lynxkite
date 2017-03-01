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
  def set(name: String, workspace: Workspace): Unit =
    controller.setWorkspace(user, SetWorkspaceRequest(name, workspace))
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

  test("validation") {
    val eg = ops.getBoxMetadata("Example Graph").toBox("eg", Map(), 0, 0)
    val ex = intercept[AssertionError] {
      Workspace(List(eg, eg))
    }
    assert(ex.getMessage.contains("Duplicate box name: eg"))
  }

  test("errors") {
    val pr1 = ops.getBoxMetadata("PageRank").toBox("pr1", Map(
      "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
      "iterations" -> "5", "direction" -> "all edges"), 0, 20)
    val pr2 = ops.getBoxMetadata("PageRank").toBox("pr2", Map(
      "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
      "iterations" -> "5", "direction" -> "all edges"), 0, 20)
    val ws = Workspace(List(pr1, pr2.connect("project", pr1.output("project"))))
    val p1 = ws.state(user, ops, pr1.output("project"))
    val p2 = ws.state(user, ops, pr2.output("project"))
    val ex1 = intercept[AssertionError] { p1.project }
    val ex2 = intercept[AssertionError] { p2.project }
    assert(ex1.getMessage.contains("Input project is not connected."))
    assert(ex2.getMessage.contains("Input project has an error."))
  }

  test("getProject") {
    using("test-workspace") {
      assert(get("test-workspace").boxes.isEmpty)
      val eg = ops.getBoxMetadata("Example Graph").toBox("eg", Map(), 0, 0)
      val ws = Workspace(List(eg))
      set("test-workspace", ws)
      val p = controller.getProject(user, GetProjectRequest("test-workspace", eg.output("project")))
      assert(p.vertexAttributes.find(_.title == "income").get.metadata("icon") == "money_bag")
    }
  }
}
