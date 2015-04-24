package com.lynxanalytics.biggraph.graph_api

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.TestTempDir

class SQLiteKeyValueStoreTest extends KeyValueStoreTest with TestTempDir {
  val store = new SQLiteKeyValueStore(tempDir("key-value-store-test").toString)
}

abstract class KeyValueStoreTest extends FunSuite {
  val store: KeyValueStore
  test("get and put") {
    store.clear
    assert(store.get("alma").isEmpty)
    store.put("alma", "korte")
    assert(store.get("alma").get == "korte")
  }

  test("delete") {
    store.clear
    store.put("alma", "korte")
    assert(store.get("alma").get == "korte")
    store.delete("alma")
    assert(store.get("alma").isEmpty)
  }

  test("scan") {
    store.clear
    assert(store.scan("alma").isEmpty)
    store.put("alma1", "korte1")
    store.put("alma2", "korte2")
    assert(store.scan("alma").toSeq ==
      Seq("alma1" -> "korte1", "alma2" -> "korte2"))
  }

  test("deletePrefix") {
    store.clear
    store.put("alma1", "korte1")
    store.put("alma2", "korte2")
    assert(store.scan("alma").toSeq ==
      Seq("alma1" -> "korte1", "alma2" -> "korte2"))
    store.deletePrefix("alma")
    assert(store.scan("alma").isEmpty)
  }

  test("batching") {
    store.clear
    store.batch {
      store.put("alma", "korte")
      assert(store.get("alma").get == "korte")
    }
    assert(store.get("alma").get == "korte")
  }
}
