package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.TestTempDir

class TagsTest extends FunSuite with TestTempDir {
  val storeFile = tempDir("TagsTest.sqlite")
  def newRoot = {
    storeFile.delete
    TagRoot(storeFile.toString)
  }
  test("We can set/read/reset a tag") {
    val root = newRoot
    root.setTag("alma/korte/barack", "hello")
    assert((root / 'alma / 'korte / 'barack).content == "hello")
    root.setTag("alma/korte/barack", "good-bye")
    assert((root / "alma/korte/barack").content == "good-bye")
  }

  test("We can clone a tag") {
    val root = newRoot
    root.setTag("alma/korte/barack", "hello")
    root.cp("alma/korte/barack", "alma/cseresznye")
    assert((root / "alma/korte/barack").content == "hello")
    assert((root / "alma/cseresznye").content == "hello")
  }

  test("We can clone a directory") {
    val root = newRoot
    root.setTag("alma/korte/barack", "hello")
    root.cp("alma", "brave_new_world/batoralma")
    assert((root / "brave_new_world/batoralma/korte/barack").content == "hello")
  }

  test("We can save and reload") {
    val root = newRoot
    root.setTag("alma/korte/barack", "hello")
    root.cp("alma", "brave_new_world/batoralma")
    val root2 = TagRoot(storeFile.toString)
    assert((root2 / "brave_new_world/batoralma/korte/barack").content == "hello")
    assert(root2.lsRec() == root.lsRec())
  }

  test("Can read tag as UUID") {
    val root = newRoot
    val uuid = UUID.randomUUID()
    root.setTag("alma/korte/barack", uuid.toString)
    assert((root / 'alma / 'korte / 'barack).gUID == uuid)
  }
}
