// The LynxKite main class to give to spark-submit.
package com.lynxanalytics.biggraph

import scala.sys.process._
import play.core.{server => pcs}

object LynxKite {
  def main(args: Array[String]): Unit = {
    start()
    // The application ends when this thread ends. (https://github.com/apache/spark/pull/32283)
    waitForever()
  }

  // Call start() and stop() from a Scala notebook to start and stop LynxKite.
  def start(): Unit = synchronized {
    assert(playServer == null, "LynxKite is already running!")
    stopping = false
    BigGraphProductionEnvironment // Initialize it here rather than risk multi-threading issues later.
    java.lang.System.setProperty("http.port", Environment.envOrElse("KITE_HTTP_PORT", "2200"))
    java.lang.System.setProperty("https.port", Environment.envOrElse("KITE_HTTPS_PORT", "2201"))
    playServer = pcs.ProdServerStart.start(new pcs.RealServerProcess(List()))
    val domainPreference = Environment.envOrElse("KITE_DOMAINS", "sphynx,spark,scala")
      .split(",").map(_.trim.toLowerCase)
    haveSphynx = domainPreference.contains("sphynx")
    if (haveSphynx) {
      sphynxStopped = false
      extractSphynx()
      val t = new Thread {
        override def run() = {
          while (!stopping) {
            val p = synchronized {
              startSphynx()
              sphynxProcess
            }
            // Wait for process exit.
            p.exitValue()
          }
          sphynxStopped = true
        }
      }
      t.setDaemon(true) // Do not block the exit if used from a script.
      t.start()
    }
  }

  def stop(): Unit = synchronized {
    if (playServer != null) {
      stopping = true
      playServer.stop()
      playServer = null
      if (haveSphynx) {
        sphynxProcess.destroy()
        sphynxProcess = null
        while (!sphynxStopped) {
          Thread.sleep(100)
        }
      }
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() = LynxKite.stop()
  })

  @volatile private var stopping = false
  @volatile private var sphynxStopped = false
  private var playServer: pcs.ReloadableServer = null
  private var haveSphynx = false
  private var sphynxProcess: Process = null

  private def waitForever(): Unit = {
    while (true) {
      Thread.sleep(365L * 24 * 3600 * 1000)
    }
  }

  // Extracts Sphynx from the jar.
  private def extractSphynx(): Unit = {
    val cl = java.lang.Thread.currentThread.getContextClassLoader
    val resource = cl.getResourceAsStream("lynxkite-sphynx.zip")
    "rm -rf lynxkite-sphynx lynxkite-sphynx.zip".!!
    java.nio.file.Files.copy(resource, java.nio.file.Paths.get("lynxkite-sphynx.zip"))
    "unzip lynxkite-sphynx.zip".!!
  }

  // Starts Sphynx and returns.
  private def startSphynx(): Process = synchronized {
    val ldLibraryPath = Environment.envOrNone("LD_LIBRARY_PATH")
    if (!ldLibraryPath.getOrElse("").startsWith(".:")) {
      Environment.set("LD_LIBRARY_PATH" -> ("." +: ldLibraryPath.toSeq).mkString(":"))
    }
    sphynxProcess = Process(
      Seq("./lynxkite-sphynx"),
      new java.io.File("lynxkite-sphynx"),
      Environment.get.toSeq: _*).run()
    sphynxProcess
  }

  def importDataFrame(df: org.apache.spark.sql.DataFrame): String = synchronized {
    assert(playServer != null, "LynxKite is not running! Call start() first.")
    val env = BigGraphProductionEnvironment
    assert(
      df.sparkSession == env.sparkSession,
      "The DataFrame is not from the SparkSession that was used to start LynxKite.")
    implicit val mm = env.metaGraphManager
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    val t = graph_operations.ImportDataFrame.run(df)
    t.gUID.toString
  }

  def getDataFrame(guid: String): org.apache.spark.sql.DataFrame = synchronized {
    import graph_api.MetaGraphManager.StringAsUUID
    assert(playServer != null, "LynxKite is not running! Call start() first.")
    val env = BigGraphProductionEnvironment
    implicit val mm = env.metaGraphManager
    val e = mm.table(guid.asUUID)
    env.dataManager.ensure(e, env.sparkDomain)
    env.dataManager.waitAllFutures()
    env.sparkDomain.getData(e).asInstanceOf[graph_api.TableData].df
  }
}
