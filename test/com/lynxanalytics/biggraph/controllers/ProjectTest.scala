package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving.User

class ProjectTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  def createProject(name: String) = {
    val controller = new BigGraphController(this)
    val request = CreateProjectRequest(name = name, notes = "", privacy = "public-write")
    controller.createProject(null, request)
    ProjectFrame.fromName(name)
  }
  val projectFrame = createProject("Test_Project")

  def undoRedo(frame: ProjectFrame) = {
    val p = SubProject(frame, Seq())
    (p.toFE.undoOp, p.toFE.redoOp)
  }

  def dodo(p: ProjectFrame, x: String) =
    p.dodo(RootProjectState.emptyState.copy(lastOperationDesc = x, checkpoint = None))

  test("Undo/redo") {
    assert(undoRedo(projectFrame) == ("", ""))
    dodo(projectFrame, "A")
    assert(undoRedo(projectFrame) == ("A", ""))
    dodo(projectFrame, "B")
    assert(undoRedo(projectFrame) == ("B", ""))
    projectFrame.undo()
    assert(undoRedo(projectFrame) == ("A", "B"))
    projectFrame.undo()
    assert(undoRedo(projectFrame) == ("", "A"))
    projectFrame.redo()
    assert(undoRedo(projectFrame) == ("A", "B"))
    dodo(projectFrame, "C")
    assert(undoRedo(projectFrame) == ("C", ""))
    projectFrame.undo()
    assert(undoRedo(projectFrame) == ("A", "C"))
    val copy = ProjectFrame.fromName("Test_Project_Copy")
    projectFrame.copy(copy)
    assert(undoRedo(copy) == ("A", "C"))
    dodo(projectFrame, "D")
    assert(undoRedo(projectFrame) == ("D", ""))
    assert(undoRedo(copy) == ("A", "C"))
    copy.redo()
    assert(undoRedo(copy) == ("C", ""))
    dodo(copy, "E")
    assert(undoRedo(copy) == ("E", ""))
    assert(undoRedo(projectFrame) == ("D", ""))
  }

  test("Segmentations") {
    val frame = ProjectFrame.fromName("1")
    frame.initialize
    val p1 = frame.viewer.editor
    val p2 = p1.segmentation("2")
    val p3 = p2.segmentation("3")
    p1.notes = "1"; p2.notes = "2"; p3.notes = "3"
    assert(!p1.isSegmentation)
    assert(p2.isSegmentation)
    assert(p3.isSegmentation)
    assert(p2.parent eq p1)
    assert(p3.parent eq p2)
    assert(p1.segmentationNames.toSet == Set("2"))
    assert(p2.segmentationNames.toSet == Set("3"))
  }

  def user(email: String) = User(email, isAdmin = false)

  def assertReaders(yes: String*)(no: String*) = {
    for (email <- yes) {
      assert(projectFrame.readAllowedFrom(user(email)))
    }
    for (email <- no) {
      assert(!projectFrame.readAllowedFrom(user(email)))
    }
  }

  def assertWriters(yes: String*)(no: String*) = {
    for (email <- yes) {
      assert(projectFrame.writeAllowedFrom(user(email)))
    }
    for (email <- no) {
      assert(!projectFrame.writeAllowedFrom(user(email)))
    }
  }

  test("Access control") {
    assertWriters("darabos@lynx", "xandrew@lynx", "forevian@andersen")()
    assertReaders("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    dodo(projectFrame, "A")
    projectFrame.writeACL = "*@lynx"
    assertWriters("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    dodo(projectFrame, "B")
    projectFrame.readACL = ""
    assertWriters("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders("darabos@lynx", "xandrew@lynx")("forevian@andersen")

    dodo(projectFrame, "C")
    projectFrame.writeACL = "darabos@lynx"
    assertWriters("darabos@lynx")("xandrew@lynx", "forevian@andersen")
    assertReaders("darabos@lynx")("xandrew@lynx", "forevian@andersen")

    dodo(projectFrame, "D")
    projectFrame.readACL = "xandrew@*"
    def lastAssert() = {
      assertWriters("darabos@lynx")("xandrew@lynx", "forevian@andersen")
      assertReaders("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    }
    lastAssert()

    // Undo/redo does not change the settings.
    projectFrame.undo()
    lastAssert()
    projectFrame.undo()
    lastAssert()
    projectFrame.undo()
    lastAssert()
    projectFrame.undo()
    lastAssert()
    projectFrame.redo()
    lastAssert()
  }
}
