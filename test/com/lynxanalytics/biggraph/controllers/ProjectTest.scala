package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving.User

class ProjectTest extends FunSuite with TestGraphOp {

  def user(email: String) = User(email, isAdmin = false)

  def assertReaders(p: DirectoryEntry)(yes: String*)(no: String*): Unit = {
    for (email <- yes) {
      assert(p.readAllowedFrom(user(email)), s"$email cannot read $p")
    }
    for (email <- no) {
      assert(!p.readAllowedFrom(user(email)), s"$email can read $p")
    }
  }

  def assertWriters(p: DirectoryEntry)(yes: String*)(no: String*): Unit = {
    for (email <- yes) {
      assert(p.writeAllowedFrom(user(email)), s"$email cannot write $p")
    }
    for (email <- no) {
      assert(!p.writeAllowedFrom(user(email)), s"$email can write $p")
    }
  }

  test("Access control") {
    implicit val mm = cleanMetaManager
    val p = DirectoryEntry.fromName("p")(mm).asNewDirectory()
    p.writeACL = "*"
    assertWriters(p)("darabos@lynx", "xandrew@lynx", "forevian@andersen")()
    assertReaders(p)("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    p.writeACL = "*@lynx"
    assertWriters(p)("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders(p)("darabos@lynx", "xandrew@lynx", "forevian@andersen")()

    p.readACL = ""
    assertWriters(p)("darabos@lynx", "xandrew@lynx")("forevian@andersen")
    assertReaders(p)("darabos@lynx", "xandrew@lynx")("forevian@andersen")

    p.writeACL = "darabos@lynx"
    assertWriters(p)("darabos@lynx")("xandrew@lynx", "forevian@andersen")
    assertReaders(p)("darabos@lynx")("xandrew@lynx", "forevian@andersen")

    p.readACL = "xandrew@*"
    assertWriters(p)("darabos@lynx")("xandrew@lynx", "forevian@andersen")
    assertReaders(p)("darabos@lynx", "xandrew@lynx")("forevian@andersen")
  }

  test("Access control with folders - write implies read") {
    implicit val mm = cleanMetaManager
    val p = DirectoryEntry.fromName("p")(mm).asNewDirectory()
    p.readACL = "x"
    p.writeACL = "*"
    assertReaders(p)("x", "y")()
    assertWriters(p)("x", "y")()
  }

  test("Access control with folders - parents checked") {
    implicit val mm = cleanMetaManager
    val p1 = DirectoryEntry.fromName("p1")(mm).asNewDirectory()
    p1.readACL = "x"
    p1.writeACL = "x"
    val p2 = DirectoryEntry.fromName("p1/p2")(mm).asNewDirectory()
    p2.readACL = "y"
    p2.writeACL = "y"
    assertReaders(p2)()("x", "y")
    assertWriters(p2)()("x", "y")

    // In parents read access is enough
    p1.readACL = "*"
    assertReaders(p2)("y")("x")
    assertWriters(p2)("y")("x")
  }

  test("Access control with folders - grandparents checked") {
    implicit val mm = cleanMetaManager
    val p1 = DirectoryEntry.fromName("p1")(mm).asNewDirectory()
    p1.readACL = "x"
    p1.writeACL = "x"
    val p2 = DirectoryEntry.fromName("p1/p2")(mm).asNewDirectory()
    p2.readACL = "y"
    p2.writeACL = "y"
    val p3 = DirectoryEntry.fromName("p1/p2/p3")(mm).asNewDirectory()
    p3.readACL = "y"
    p3.writeACL = "y"
    assertReaders(p3)()("x", "y")
    assertWriters(p3)()("x", "y")

    // Write access should imply read access in parents too
    p1.writeACL = "*"
    assertReaders(p3)("y")("x")
    assertWriters(p3)("y")("x")
  }

  test("Invalid path error message") {
    implicit val mm = cleanMetaManager
    DirectoryEntry.fromName("dir")(mm).asNewDirectory()
    val frame = DirectoryEntry.fromName("dir/project")(mm).asNewWorkspaceFrame()
    frame.initialize
    val e = intercept[java.lang.AssertionError] {
      DirectoryEntry.fromName("dir/project/p")(mm).asNewWorkspaceFrame()
    }
    assert(e.getMessage.contains(
      "Invalid path: dir/project/p. Parent dir/project is not a directory."))
  }

  test("Path validation") {
    implicit val mm = cleanMetaManager
    DirectoryEntry.fromName("users/mr.potato@example.com/dir")(mm).asNewDirectory()
    val e = intercept[java.lang.AssertionError] {
      DirectoryEntry.fromName("users/mr.potato@example.com/!dir")(mm).asNewDirectory()
    }
  }
}
