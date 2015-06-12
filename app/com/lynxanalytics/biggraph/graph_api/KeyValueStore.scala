// A persistent key-value storage interface and implementation(s).
package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import java.io.File
import com.fasterxml.jackson.core.JsonProcessingException
import play.api.libs.json.Json

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
  val Error = "CorruptedInput"

  def readAll: Iterable[(String, String)] = {
    import scala.collection.JavaConverters._
    val data = new java.util.TreeMap[String, String]
    var errorAtEnd = false
    for ((command, key, value) <- readCommands) {
      command match {
        case Error =>
          errorAtEnd = true;
          log.info(
            s"Bad input line: '$key' in file: '$file' " +
              s"""json error: ${value.replaceAll("\\n", " ")}""")
        case Put => errorAtEnd = false; data.put(key, value)
        case Delete => errorAtEnd = false; data.remove(key)
        case DeletePrefix => errorAtEnd = false; data.subMap(key, key + Z).entrySet.clear
      }
    }

    if (errorAtEnd) {
      out.newLine()
      out.flush()
      log.warn(s"Error was found at end of '$file' appending newline")
    }
    data.asScala
  }

  def readCommands: Iterable[(String, String, String)] = {
    def readStream(in: java.io.BufferedReader): Stream[(String, String, String)] = {
      val line = in.readLine
      if (line == null) Stream.empty // End of file reached.
      else {
        val j =
          try {
            Json.parse(line).as[Seq[String]]
          } catch {
            // Sorry, we'll use the existing interface to convey information about
            // this awkward situation to the caller.
            case e: JsonProcessingException => Seq[String](Error, line, e.getMessage)
          }
        (j(0), j(1), j(2)) #:: readStream(in)
      }
    }
    if (new File(file).exists) {
      val in = java.nio.file.Files.newBufferedReader(
        java.nio.file.Paths.get(file),
        java.nio.charset.StandardCharsets.UTF_8)
      readStream(in)
    } else Seq()
  }

  private def write(command: String, key: String, value: String = "") = synchronized {
    if (doWrites) {
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
      flushing = true
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
