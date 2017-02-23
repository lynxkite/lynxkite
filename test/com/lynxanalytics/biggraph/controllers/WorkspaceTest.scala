package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

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
  def assertBoxesArrowsStates(ws: Workspace, boxes: Int, arrows: Int, states: Int) = {
    assert(ws.boxes.length == boxes)
    assert(ws.arrows.length == arrows)
    assert(ws.states.length == states)
  }
  case class AddBox(
      start: Workspace, op: String, params: Map[String, String] = Map(),
      x: Double = 0, y: Double = 0) {
    val box = start.autoName(ops.getBoxMetadata(op).toBox(params, x, y))
    val ws = start.addBox(box).fillStates(user, ops)
  }
  def addArrow(
    ws: Workspace, src: BoxConnection, dst: BoxConnection): Workspace = {
    ws.addArrows(Seq(Arrow(src, dst))).fillStates(user, ops)
  }

  test("pagerank on example graph") {
    using("test-workspace") {
      val blank = get("test-workspace")
      // Empty workspace.
      assertBoxesArrowsStates(blank, 0, 0, 0)
      val eg = AddBox(blank, "Example Graph", y = 20)
      // Added example graph. Its output is computed.
      assertBoxesArrowsStates(eg.ws, 1, 0, 1)
      val pr = AddBox(
        eg.ws, "PageRank", y = 60,
        params = Map(
          "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
          "iterations" -> "5", "direction" -> "all edges"))
      // Added PageRank. It is not yet connected, so its output is not computed.
      assertBoxesArrowsStates(pr.ws, 2, 0, 1)
      val connected = addArrow(pr.ws, eg.box.output("project"), pr.box.input("project"))
      assert(connected.boxes(1) == pr.box)
      // PageRank is now connected and its output computed.
      assertBoxesArrowsStates(connected, 2, 1, 2)
      val project = connected.stateMap(pr.box.output("project")).project
      import graph_api.Scripting._
      assert(project.vertexAttributes("pagerank").rdd.values.collect.toSet == Set(
        1.4099834026132592, 1.4099834026132592, 0.9892062327983842, 0.19082696197509774))
    }
  }
}
