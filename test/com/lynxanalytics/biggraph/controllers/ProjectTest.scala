package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving.User

class ProjectTest extends FunSuite with TestGraphOp {
  def createProject(name: String) = {
    val controller = new BigGraphController(this)
    val request = CreateProjectRequest(name = name, notes = "", privacy = "public-write")
    controller.createProject(User.fake, request)
    ProjectFrame.fromName(name)
  }
  val projectFrame = createProject("Test_Project")

  def undoRedo(frame: ProjectFrame) = {
    val p = frame.subproject
    (p.toFE.undoOp, p.toFE.redoOp)
  }

  def addFakeCheckpoint(p: ProjectFrame, x: String) =
    p.setCheckpoint(metaGraphManager.checkpointRepo.checkpointState(
      RootProjectState.emptyState.copy(lastOperationDesc = x, checkpoint = None),
      p.checkpoint).checkpoint.get)

  test("Undo/redo") {
    assert(undoRedo(projectFrame) == ("", ""))
    addFakeCheckpoint(projectFrame, "A")
    assert(undoRedo(projectFrame) == ("A", ""))
    addFakeCheckpoint(projectFrame, "B")
    assert(undoRedo(projectFrame) == ("B", ""))
    projectFrame.undo()
    assert(undoRedo(projectFrame) == ("A", "B"))
    projectFrame.undo()
    assert(undoRedo(projectFrame) == ("", "A"))
    projectFrame.redo()
    assert(undoRedo(projectFrame) == ("A", "B"))
    addFakeCheckpoint(projectFrame, "C")
    assert(undoRedo(projectFrame) == ("C", ""))
    projectFrame.undo()
    assert(undoRedo(projectFrame) == ("A", "C"))
    val copy = projectFrame.copy(DirectoryEntry.fromName("Test_Project_Copy"))
    assert(undoRedo(copy) == ("A", "C"))
    addFakeCheckpoint(projectFrame, "D")
    assert(undoRedo(projectFrame) == ("D", ""))
    assert(undoRedo(copy) == ("A", "C"))
    copy.redo()
    assert(undoRedo(copy) == ("C", ""))
    addFakeCheckpoint(copy, "E")
    assert(undoRedo(copy) == ("E", ""))
    assert(undoRedo(projectFrame) == ("D", ""))
  }

  test("Segmentations") {
    val frame = DirectoryEntry.fromName("1").asNewProjectFrame()
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

  def assertReaders(yes: String*)(no: String*): Unit = {
    assertProjectReaders(projectFrame)(yes: _*)(no: _*)
  }

  def assertProjectReaders(p: DirectoryEntry)(yes: String*)(no: String*): Unit = {
    for (email <- yes) {
      assert(p.readAllowedFrom(user(email)), s"$email cannot read $p")
    }
    for (email <- no) {
      assert(!p.readAllowedFrom(user(email)), s"$email can read $p")
    }
  }

  def assertWriters(yes: String*)(no: String*): Unit = {
    assertProjectWriters(projectFrame)(yes: _*)(no: _*)
  }

  def assertProjectWriters(p: DirectoryEntry)(yes: String*)(no: String*): Unit = {
    for (email <- yes) {
      assert(p.writeAllowedFrom(user(email)), s"$email cannot write $p")
    }
    for (email <- no) {
      assert(!p.writeAllowedFrom(user(email)), s"$email can write $p")
    }
  }

  test("Access control") {
    assertWriters("darabos@lynx", "xandrew@lynx", "forevian@andersen")()
    assertReaders("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    addFakeCheckpoint(projectFrame, "A")
    projectFrame.writeACL = "*@lynx"
    assertWriters("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    addFakeCheckpoint(projectFrame, "B")
    projectFrame.readACL = ""
    assertWriters("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders("darabos@lynx", "xandrew@lynx")("forevian@andersen")

    addFakeCheckpoint(projectFrame, "C")
    projectFrame.writeACL = "darabos@lynx"
    assertWriters("darabos@lynx")("xandrew@lynx", "forevian@andersen")
    assertReaders("darabos@lynx")("xandrew@lynx", "forevian@andersen")

    addFakeCheckpoint(projectFrame, "D")
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

  test("Access control with folders - write implies read") {
    val p = DirectoryEntry.fromName("p").asNewDirectory()
    p.readACL = "x"
    p.writeACL = "*"
    assertProjectReaders(p)("x", "y")()
    assertProjectWriters(p)("x", "y")()
  }

  test("Access control with folders - parents checked") {
    val p1 = DirectoryEntry.fromName("p1").asNewDirectory()
    p1.readACL = "x"
    p1.writeACL = "x"
    val p2 = DirectoryEntry.fromName("p1/p2").asNewDirectory()
    p2.readACL = "y"
    p2.writeACL = "y"
    assertProjectReaders(p2)()("x", "y")
    assertProjectWriters(p2)()("x", "y")

    // In parents read access is enough
    p1.readACL = "*"
    assertProjectReaders(p2)("y")("x")
    assertProjectWriters(p2)("y")("x")
  }

  test("Access control with folders - grandparents checked") {
    val p1 = Directory.fromName("p1")
    p1.readACL = "x"
    p1.writeACL = "x"
    val p2 = Directory.fromName("p1/p2")
    p2.readACL = "y"
    p2.writeACL = "y"
    val p3 = DirectoryEntry.fromName("p1/p2/p3").asNewDirectory()
    p3.readACL = "y"
    p3.writeACL = "y"
    assertProjectReaders(p3)()("x", "y")
    assertProjectWriters(p3)()("x", "y")

    // Write access should imply read access in parents too
    p1.writeACL = "*"
    assertProjectReaders(p3)("y")("x")
    assertProjectWriters(p3)("y")("x")
  }

  test("Invalid path error message") {
    DirectoryEntry.fromName("dir").asNewDirectory()
    val frame = DirectoryEntry.fromName("dir/project").asNewProjectFrame()
    frame.initialize
    val e = intercept[java.lang.AssertionError] {
      DirectoryEntry.fromName("dir/project/p").asNewProjectFrame()
    }
    assert(e.getMessage.contains(
      "Invalid path: dir/project/p. Parent dir/project is not a directory."))
  }
}
