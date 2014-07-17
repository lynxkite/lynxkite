package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.scalatest.FunSuite

class TagsTest extends FunSuite {
  test("We can set/read/reset a tag") {
    val root = TagRoot()
    val uuid = UUID.randomUUID()
    root.setTag("alma/korte/barack", uuid)
    assert((root / 'alma / 'korte / 'barack).gUID == uuid)
    val uuid2 = UUID.randomUUID()
    root.setTag("alma/korte/barack", uuid2)
    assert((root / "alma/korte/barack").gUID == uuid2)
  }
  test("We can clone a tag") {
    val root = TagRoot()
    val uuid = UUID.randomUUID()
    root.setTag("alma/korte/barack", uuid)
    root.cp("alma/korte/barack", "alma/cseresznye")
    assert((root / "alma/korte/barack").gUID == uuid)
    assert((root / "alma/cseresznye").gUID == uuid)
  }
  test("We can clone a directory") {
    val root = TagRoot()
    val uuid = UUID.randomUUID()
    root.setTag("alma/korte/barack", uuid)
    root.cp("alma", "brave_new_world/batoralma")
    assert((root / "brave_new_world/batoralma/korte/barack").gUID == uuid)
  }
  test("We can save and reload") {
    val root = TagRoot()
    val uuid = UUID.randomUUID()
    root.setTag("alma/korte/barack", uuid)
    root.cp("alma", "brave_new_world/batoralma")
    println(root.saveToString)
    val root2 = TagRoot()
    root2.loadFromString(root.saveToString)
    assert((root2 / "brave_new_world/batoralma/korte/barack").gUID == uuid)
    assert(root2.lsRec == root.lsRec)
  }
}
