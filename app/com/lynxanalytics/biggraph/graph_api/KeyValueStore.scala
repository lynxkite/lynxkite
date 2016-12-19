// A persistent key-value storage interface and implementation(s).
package com.lynxanalytics.biggraph.graph_api

import com.fasterxml.jackson.core.JsonProcessingException
import java.io.File
import play.api.libs.json.Json

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

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
  new File(file).getParentFile.mkdirs // Create directory if necessary.
  private val out = java.nio.file.Files.newBufferedWriter(
    java.nio.file.Paths.get(file),
    java.nio.charset.StandardCharsets.UTF_8,
    java.nio.file.StandardOpenOption.APPEND,
    java.nio.file.StandardOpenOption.CREATE)
  // Journal entry types.
  val Put = "Put"
  val Delete = "Delete"
  val DeletePrefix = "DeletePrefix"

  def readAll: Iterable[(String, String)] = {
    import scala.collection.JavaConverters._
    val data = new java.util.TreeMap[String, String]
    forCommands { (command, key, value) =>
      command match {
        case Put => data.put(key, value)
        case Delete => data.remove(key)
        case DeletePrefix => data.subMap(key, key + Z).entrySet.clear
      }
    }
    data.asScala
  }

  def forCommands[T](f: (String, String, String) => T): Unit = {
    if (new File(file).exists) {
      for (line <- scala.io.Source.fromFile(file, "utf-8").getLines) {
        if (line.nonEmpty) {
          try {
            val j = Json.parse(line).as[Seq[String]]
            if (!j(1).contains("!tmp")) {
              f(j(0), j(1), j(2))
            }
          } catch {
            case e: JsonProcessingException =>
              log.warn(s"Bad input line: '$line' in file: '$file' " +
                s"""json error: ${e.getMessage.replaceAll("\\n", " ")}""")
            case e: Throwable =>
              log.error(s"Bad input line: '$line' in file: '$file'")
              throw e
          }
        }
      }
    }
  }

  private var streamInited = false
  private def initStreamAtStartup() = {
    if (!streamInited) {
      out.newLine()
      out.flush()
      streamInited = true
    }
  }

  private def write(command: String, key: String, value: String = "") = synchronized {
    if (doWrites) {
      initStreamAtStartup()
      out.write(Json.toJson(Seq(command, key, value)).toString)
      out.newLine()
      if (flushing) out.flush()
    }
  }

  def put(key: String, value: String): Unit = synchronized {
    write(Put, key, value)
  }

  def delete(key: String): Unit = synchronized {
    write(Delete, key)
  }

  def deletePrefix(prefix: String): Unit = synchronized {
    write(DeletePrefix, prefix)
  }

  private var flushing = true
  def batch[T](fn: => T): T = synchronized {
    val f = flushing
    flushing = false
    try {
      fn
    } finally {
      flushing = f
      out.flush()
    }
  }

  private var ignoreWrites = 0
  private def doWrites = synchronized { ignoreWrites == 0 }
  def writesCanBeIgnored[T](fn: => T): T = synchronized {
    ignoreWrites += 1
    try { fn }
    finally { ignoreWrites -= 1 }
  }
}

case class JsonKeyValueStore(file: String) extends KeyValueStore {
  import org.apache.commons.io.FileUtils

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
