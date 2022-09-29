package com.lynxanalytics.lynxkite.graph_api

import org.scalatest.funsuite.AnyFunSuite
import com.lynxanalytics.lynxkite.TestTempDir

class JournalKeyValueStoreTest extends KeyValueStoreTest {
  def newStore = {
    storeFile.delete()
    new JournalKeyValueStore(storeFile.toString)
  }
}

abstract class KeyValueStoreTest extends AnyFunSuite with TestTempDir {
  val storeFile = tempDir("key-value-store-test")
  def newStore: KeyValueStore

  test("put") {
    val s = newStore
    assert(s.readAll.isEmpty)
    s.put("alma", "korte")
    assert(s.readAll == Map("alma" -> "korte"))
  }

  test("delete") {
    val s = newStore
    s.put("beka", "malac")
    s.put("csiga", "nyuszi")
    assert(s.readAll == Map("csiga" -> "nyuszi", "beka" -> "malac"))
    s.delete("beka")
    assert(s.readAll == Map("csiga" -> "nyuszi"))
  }

  test("deletePrefix") {
    val s = newStore
    s.put("alma", "korte")
    s.put("beka", "malac")
    s.put("alma2", "korte2")
    assert(s.readAll.size == 3)
    s.deletePrefix("alma")
    assert(s.readAll == Map("beka" -> "malac"))
  }
}
