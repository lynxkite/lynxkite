package com.lynxanalytics.lynxkite.graph_api

import java.util.UUID
import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite
import com.lynxanalytics.lynxkite.TestTempDir

class TagsTest extends AnyFunSuite with TestTempDir {
  val storeFile = tempDir("TagsTest")
  def newRoot = {
    FileUtils.deleteDirectory(storeFile)
    TagRoot(storeFile.toString)
  }
  test("We can set/read/reset a tag") {
    val root = newRoot
    root.setTag(SymbolPath.parse("alma/korte/barack"), "hello")
    assert((root / SymbolPath('alma, 'korte, 'barack)).content == "hello")
    root.setTag(SymbolPath.parse("alma/korte/barack"), "good-bye")
    assert((root / SymbolPath.parse("alma/korte/barack")).content == "good-bye")
  }

  test("We can clone a tag") {
    val root = newRoot
    val src = SymbolPath.parse("alma/korte/barack")
    val dst = SymbolPath.parse("alma/cseresznye")
    root.setTag(src, "hello")
    root.cp(src, dst)
    assert((root / src).content == "hello")
    assert((root / dst).content == "hello")
  }

  test("We can clone a directory") {
    val root = newRoot
    root.setTag(SymbolPath.parse("alma/korte/barack"), "hello")
    root.cp(SymbolPath("alma"), SymbolPath.parse("brave_new_world/batoralma"))
    assert((root /
      SymbolPath.parse("brave_new_world/batoralma/korte/barack")).content == "hello")
  }

  test("We can save and reload") {
    val root = newRoot
    root.setTag(SymbolPath.parse("alma/korte/barack"), "hello")
    root.cp(SymbolPath("alma"), SymbolPath.parse("brave_new_world/batoralma"))
    val root2 = TagRoot(storeFile.toString)
    assert((root2 /
      SymbolPath.parse("brave_new_world/batoralma/korte/barack")).content == "hello")
    assert(root2.lsRec() == root.lsRec())
  }

  test("Directory deletion works") {
    val root = newRoot
    val path = SymbolPath.parse("alma/korte/barack")
    root.setTag(path, "hello")
    assert(root.exists(path))
    root.rm(SymbolPath.parse("alma/korte"))
    assert(!root.exists(path))
    val root2 = TagRoot(storeFile.toString)
    assert(!root2.exists(path))
  }

  test("Can read tag as UUID") {
    val root = newRoot
    val uuid = UUID.randomUUID()
    root.setTag(SymbolPath.parse("alma/korte/barack"), uuid.toString)
    assert((root / SymbolPath('alma, 'korte, 'barack)).gUID == uuid)
  }
}
