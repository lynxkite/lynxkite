package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving.User

class WorkspaceTest extends FunSuite with TestGraphOp {
  val controller = new BigGraphController(this)
  val user = com.lynxanalytics.biggraph.serving.User.fake

  def create(name: String) =
    controller.createWorkspace(user, CreateWorkspaceRequest(name, "private"))
  def get(name: String) =
    controller.getWorkspace(user, GetWorkspaceRequest(name))
  def discard(name: String) =
    controller.discardEntry(user, DiscardEntryRequest(name))
  def using[T](name: String)(f: => T): T =
    try {
      create(name)
      f
    } finally discard(name)

  test("empty workspace is empty") {
    using("test-workspace") {
      val blank = get("test-workspace")
      assert(blank.boxes.length == 0)
      assert(blank.arrows.length == 0)
      assert(blank.states.length == 0)
    }
  }

  ignore("pagerank on example graph") {
  }
}
