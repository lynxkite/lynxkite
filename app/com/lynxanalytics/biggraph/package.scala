// Package-level variables. Creates our logger and the BigGraphEnvironment.
package com.lynxanalytics

import com.lynxanalytics.biggraph.graph_util.{LoggedEnvironment, PrefixRepository}
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._

package object biggraph {
  val bigGraphLogger = LoggerFactory.getLogger("LynxKite")

  val standardDataPrefix = "DATA$"

  def registerStandardPrefixes() = {
    PrefixRepository.registerPrefix("UPLOAD$", standardDataPrefix + "/uploads")
  }

  lazy val BigGraphProductionEnvironment: BigGraphEnvironment = {
    // Make sure play and spark logs contain the proper context.
    val ctx = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val frameworkPackages = ctx.getFrameworkPackages
    frameworkPackages.add("play.api.Logger")
    frameworkPackages.add("org.apache.spark.Logging")

    bigGraphLogger.info("Starting to initialize production Kite environment")
    def clean(s: String) = s.reverse.dropWhile(_ == '/').reverse // Drop trailing slashes.
    val repoDirs = {
      val metaDir =
        clean(LoggedEnvironment.envOrError(
          "KITE_META_DIR",
          "Please set KITE_META_DIR and KITE_DATA_DIR."))
      val dataDir =
        clean(LoggedEnvironment.envOrError(
          "KITE_DATA_DIR",
          "Please set KITE_DATA_DIR.",
          confidential = true))
      val ephemeralDataDir =
        LoggedEnvironment.envOrNone("KITE_EPHEMERAL_DATA_DIR", confidential = true).map(clean)
      new RepositoryDirs(metaDir, standardDataPrefix, dataDir, ephemeralDataDir)
    }
    repoDirs.forcePrefixRegistration()
    registerStandardPrefixes()

    val res = BigGraphEnvironmentImpl.createStaticDirEnvironment(
      repoDirs,
      new StaticSparkSessionProvider())
    bigGraphLogger.info("Production Kite environment initialized")
    res
  }
}
