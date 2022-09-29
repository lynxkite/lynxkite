// Package-level variables. Creates our logger and the BigGraphEnvironment.
package com.lynxanalytics

import com.lynxanalytics.biggraph.graph_util.{Environment, PrefixRepository}
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._

package object biggraph {
  val bigGraphLogger = LoggerFactory.getLogger("LynxKite")

  val standardDataPrefix = "DATA$"

  def registerStandardPrefixes() = {
    PrefixRepository.registerPrefix("UPLOAD$", standardDataPrefix + "/uploads")
  }

  lazy val BigGraphProductionEnvironment: BigGraphEnvironment = {
    bigGraphLogger.info("Starting to initialize production Kite environment")
    def clean(s: String) = s.reverse.dropWhile(_ == '/').reverse // Drop trailing slashes.
    val repoDirs = {
      val metaDir =
        clean(Environment.envOrError(
          "KITE_META_DIR",
          "Please set KITE_META_DIR and KITE_DATA_DIR."))
      val dataDir =
        clean(Environment.envOrError(
          "KITE_DATA_DIR",
          "Please set KITE_DATA_DIR.",
          confidential = true))
      val ephemeralDataDir =
        Environment.envOrNone("KITE_EPHEMERAL_DATA_DIR", confidential = true).map(clean)
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
