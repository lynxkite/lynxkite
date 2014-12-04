package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ProjectTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  def createProject(name: String) = {
    val controller = new BigGraphController(this)
    val request = CreateProjectRequest(name = name, notes = name, privacy = "public-write")
    controller.createProject(null, request)
    Project(name)
  }
  val project = createProject("Test_Project")

  def undoRedo(p: Project) = (p.toFE.undoOp, p.toFE.redoOp)

  test("Undo/redo") {
    assert(undoRedo(project) == ("", ""))
    project.checkpointAfter("A")
    assert(undoRedo(project) == ("A", ""))
    project.checkpointAfter("B")
    assert(undoRedo(project) == ("B", ""))
    project.undo()
    assert(undoRedo(project) == ("A", "B"))
    project.undo()
    assert(undoRedo(project) == ("", "A"))
    project.redo()
    assert(undoRedo(project) == ("A", "B"))
    project.checkpointAfter("C")
    assert(undoRedo(project) == ("C", ""))
    project.undo()
    assert(undoRedo(project) == ("A", "C"))
    val copy = Project("Test_Project_Copy")
    project.copy(copy)
    assert(undoRedo(copy) == ("A", "C"))
    project.checkpointAfter("D")
    assert(undoRedo(project) == ("D", ""))
    copy.redo()
    assert(undoRedo(copy) == ("C", ""))
    copy.checkpointAfter("E")
    assert(undoRedo(copy) == ("E", ""))
    assert(undoRedo(project) == ("D", ""))
  }
}
