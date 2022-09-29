// The LynxKite main class to give to spark-submit.
package com.lynxanalytics.biggraph

import scala.sys.process._
import com.lynxanalytics.biggraph.graph_util.Environment

object Main {
  def main(args: Array[String]): Unit = {
    BigGraphProductionEnvironment // Initialize it here rather than risk multi-threading issues later.
    play.core.server.ProdServerStart.main(args)
    val domainPreference = Environment.envOrElse("KITE_DOMAINS", "sphynx,spark,scala")
      .split(",").map(_.trim.toLowerCase)
    if (domainPreference.contains("sphynx")) {
      extractSphynx()
      while (true) {
        runSphynx()
      }
    } else {
      // The application ends when this thread ends. (https://github.com/apache/spark/pull/32283)
      while (true) {
        Thread.sleep(365L * 24 * 3600 * 1000)
      }
    }
  }
  // Extracts Sphynx from the jar.
  def extractSphynx(): Unit = {
    val cl = java.lang.Thread.currentThread.getContextClassLoader
    val resource = cl.getResourceAsStream("lynxkite-sphynx.zip")
    "rm -rf lynxkite-sphynx lynxkite-sphynx.zip".!!
    java.nio.file.Files.copy(resource, java.nio.file.Paths.get("lynxkite-sphynx.zip"))
    "unzip lynxkite-sphynx.zip".!!
  }
  // Runs Sphynx, blocking until it exits.
  def runSphynx(): Int = {
    Process(
      Seq("./lynxkite-sphynx"),
      new java.io.File("lynxkite-sphynx"),
      "LD_LIBRARY_PATH" -> ".").!
  }
}
