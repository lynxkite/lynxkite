// A persistent key-value storage interface and implementation(s).
package com.lynxanalytics.biggraph.graph_api

trait KeyValueStore {
  def readAll: Iterable[(String, String)]
  def delete(key: String): Unit
  def put(key: String, value: String): Unit
  def deletePrefix(prefix: String): Unit
  def batch[T](fn: => T): T // Can be nested. Defer writes until the end.
  def writesCanBeIgnored[T](fn: => T): T // May ignore writes from "fn".
}

case class JournalKeyValueStore(file: String) extends KeyValueStore {
  private val Z = '\uffff' // Highest character code.
  private val out = new java.io.DataOutputStream(
    new java.io.FileOutputStream(file, /* append = */ true))

  def readAll: Iterable[(String, String)] = {
    import scala.collection.JavaConverters._
    val data = new java.util.TreeMap[String, String]
    for ((command, key, value) <- readCommands) {
      command match {
        case "put" => data.put(key, value)
        case "delete" => data.remove(key)
        case "deletePrefix" => data.subMap(key, key + Z).entrySet.clear
      }
    }
    data.asScala
  }

  def readCommands: Iterable[(String, String, String)] = {
    val in = new java.io.DataInputStream(
      new java.io.FileInputStream(file))
    def readStream: Stream[(String, String, String)] = {
      try {
        val command = in.readUTF
        val key = in.readUTF
        val value = in.readUTF
        (command, key, value) #:: readStream
      } catch {
        case e: java.io.EOFException =>
          in.close()
          Stream.empty
      }
    }
    readStream
  }

  private def write(command: String, key: String, value: String = "") = {
    out.writeUTF(command)
    out.writeUTF(key)
    out.writeUTF(value)
    if (flushing) out.flush()
  }

  def delete(key: String): Unit = if (doWrites) synchronized {
    write("delete", key)
  }

  def put(key: String, value: String): Unit = if (doWrites) synchronized {
    write("put", key, value)
  }

  def deletePrefix(prefix: String): Unit = if (doWrites) synchronized {
    write("deletePrefix", prefix)
  }

  private var flushing = true
  def batch[T](fn: => T): T = synchronized {
    val f = flushing
    flushing = false
    try {
      fn
    } finally {
      flushing = true
      out.flush()
    }
  }

  private var ignoreWrites = 0
  private def doWrites = ignoreWrites == 0
  def writesCanBeIgnored[T](fn: => T): T = {
    ignoreWrites += 1
    try { fn }
    finally { ignoreWrites -= 1 }
  }
}

case class JsonKeyValueStore(file: String) extends KeyValueStore {
  import java.io.File
  import org.apache.commons.io.FileUtils
  import play.api.libs.json.Json

  private val raw = FileUtils.readFileToString(new File(file), "utf8")
  private val map = Json.parse(raw).as[Map[String, String]]

  def readAll: Iterable[(String, String)] = map

  // This is a read-only implementation, used for backward-compatibility.
  def delete(key: String): Unit = ???
  def put(key: String, value: String): Unit = ???
  def deletePrefix(prefix: String): Unit = ???
  def batch[T](fn: => T): T = ???
  def writesCanBeIgnored[T](fn: => T): T = fn
}
