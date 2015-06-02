package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.TestTempDir

class TagsTest extends FunSuite with TestTempDir {
  val storeFile = tempDir("TagsTest")
  def newRoot = {
    FileUtils.deleteDirectory(storeFile)
    TagRoot(storeFile.toString)
  }
  test("We can set/read/reset a tag") {
    val root = newRoot
    root.setTag(SymbolPath.fromString("alma/korte/barack"), "hello")
    assert((root / (SymbolPath.fromSymbol('alma) / 'korte / 'barack)).content == "hello")
    root.setTag(SymbolPath.fromString("alma/korte/barack"), "good-bye")
    assert((root / SymbolPath.fromString("alma/korte/barack")).content == "good-bye")
  }

  test("We can clone a tag") {
    val root = newRoot
    val src = SymbolPath.fromString("alma/korte/barack")
    val dst = SymbolPath.fromString("alma/cseresznye")
    root.setTag(src, "hello")
    root.cp(src, dst)
    assert((root / src).content == "hello")
    assert((root / dst).content == "hello")
  }

  test("We can clone a directory") {
    val root = newRoot
    root.setTag(SymbolPath.fromString("alma/korte/barack"), "hello")
    root.cp(SymbolPath.fromString("alma"), SymbolPath.fromString("brave_new_world/batoralma"))
    assert((root /
      SymbolPath.fromString("brave_new_world/batoralma/korte/barack")).content == "hello")
  }

  test("We can save and reload") {
    val root = newRoot
    root.setTag(SymbolPath.fromString("alma/korte/barack"), "hello")
    root.cp(SymbolPath.fromString("alma"), SymbolPath.fromString("brave_new_world/batoralma"))
    val root2 = TagRoot(storeFile.toString)
    assert((root2 /
      SymbolPath.fromString("brave_new_world/batoralma/korte/barack")).content == "hello")
    assert(root2.lsRec() == root.lsRec())
  }

  test("Directory deletion works") {
    val root = newRoot
    val path = SymbolPath.fromString("alma/korte/barack")
    root.setTag(path, "hello")
    assert(root.exists(path))
    root.rm(SymbolPath.fromString("alma/korte"))
    assert(!root.exists(path))
    val root2 = TagRoot(storeFile.toString)
    assert(!root2.exists(path))
  }

  test("Can read tag as UUID") {
    val root = newRoot
    val uuid = UUID.randomUUID()
    root.setTag(SymbolPath.fromString("alma/korte/barack"), uuid.toString)
    assert((root / (SymbolPath.fromSymbol('alma) / 'korte / 'barack)).gUID == uuid)
  }
}
