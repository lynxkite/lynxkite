package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.scalatest.FunSuite

class TagsTest extends FunSuite {
  test("We can set/read/reset a tag") {
    val root = TagRoot()
    root.setTag("alma/korte/barack", "hello")
    assert((root / 'alma / 'korte / 'barack).content == "hello")
    root.setTag("alma/korte/barack", "good-bye")
    assert((root / "alma/korte/barack").content == "good-bye")
  }

  test("We can clone a tag") {
    val root = TagRoot()
    root.setTag("alma/korte/barack", "hello")
    root.cp("alma/korte/barack", "alma/cseresznye")
    assert((root / "alma/korte/barack").content == "hello")
    assert((root / "alma/cseresznye").content == "hello")
  }

  test("We can clone a directory") {
    val root = TagRoot()
    root.setTag("alma/korte/barack", "hello")
    root.cp("alma", "brave_new_world/batoralma")
    assert((root / "brave_new_world/batoralma/korte/barack").content == "hello")
  }

  test("We can save and reload") {
    val root = TagRoot()
    root.setTag("alma/korte/barack", "hello")
    root.cp("alma", "brave_new_world/batoralma")
    val root2 = TagRoot()
    root2.loadFromString(root.saveToString)
    assert((root2 / "brave_new_world/batoralma/korte/barack").content == "hello")
    assert(root2.lsRec() == root.lsRec())
  }

  test("Can read tag as UUID") {
    val root = TagRoot()
    val uuid = UUID.randomUUID()
    root.setTag("alma/korte/barack", uuid.toString)
    assert((root / 'alma / 'korte / 'barack).gUID == uuid)
  }
}
