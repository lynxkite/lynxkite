package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import securesocial.{ core => ss }

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

  test("Segmentations") {
    val p1 = Project("1")
    val p2 = p1.segmentation("2").project
    val p3 = p2.segmentation("3").project
    p1.notes = "1"; p2.notes = "2"; p3.notes = "3"
    assert(!p1.isSegmentation)
    assert(p2.isSegmentation)
    assert(p3.isSegmentation)
    assert(p2.asSegmentation.parent == p1)
    assert(p3.asSegmentation.parent == p2)
    assert(p1.segmentations == Seq(p2.asSegmentation))
    assert(p2.segmentations == Seq(p3.asSegmentation))
  }

  def user(email: String) = {
    ss.SocialUser(
      ss.IdentityId(email, ss.providers.UsernamePasswordProvider.UsernamePassword),
      email, email, email, Some(email), None, ss.AuthenticationMethod.UserPassword)
  }

  def assertReaders(yes: String*)(no: String*) = {
    for (email <- yes) {
      assert(project.readAllowedFrom(user(email)))
    }
    for (email <- no) {
      assert(!project.readAllowedFrom(user(email)))
    }
  }

  def assertWriters(yes: String*)(no: String*) = {
    for (email <- yes) {
      assert(project.writeAllowedFrom(user(email)))
    }
    for (email <- no) {
      assert(!project.writeAllowedFrom(user(email)))
    }
  }

  test("Access control") {
    assertWriters("darabos@lynx", "xandrew@lynx", "forevian@andersen")()
    assertReaders("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    project.checkpointAfter("A")
    project.writeACL = "*@lynx"
    assertWriters("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    project.checkpointAfter("B")
    project.readACL = ""
    assertWriters("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders("darabos@lynx", "xandrew@lynx")("forevian@andersen")

    project.checkpointAfter("C")
    project.writeACL = "darabos@lynx"
    assertWriters("darabos@lynx")("xandrew@lynx", "forevian@andersen")
    assertReaders("darabos@lynx")("xandrew@lynx", "forevian@andersen")

    project.checkpointAfter("D")
    project.readACL = "xandrew@*"
    def lastAssert() = {
      assertWriters("darabos@lynx")("xandrew@lynx", "forevian@andersen")
      assertReaders("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    }
    lastAssert()

    // Undo/redo does not change the settings.
    project.undo()
    lastAssert()
    project.undo()
    lastAssert()
    project.undo()
    lastAssert()
    project.undo()
    lastAssert()
    project.redo()
    lastAssert()
    project.reloadCurrentCheckpoint()
    lastAssert()
  }
}
