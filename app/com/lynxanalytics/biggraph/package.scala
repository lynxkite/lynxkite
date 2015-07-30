// Package-level variables. Creates our logger and the BigGraphEnvironment.
package com.lynxanalytics

import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._

package object biggraph {
  val bigGraphLogger = LoggerFactory.getLogger("LynxKite")

  // Initialize reflection to avoid thread-safety issues
  // TODO: ditch this when we get to Scala 2.11
  def printType[T: TypeTag]: Unit = bigGraphLogger.debug("initialize reflection for type: " + typeOf[T])

  printType[Long]
  printType[String]
  printType[Double]
  printType[Array[Long]]

  // static<meta_dir,data_dir,ephemeral_data_dir>
  private val staticRepoPattern = "static<(.+),(.+),(.+)>".r

  val standardDataPrefix = "DATA$"

  def registerStandardPrefixes() = {
    PrefixRepository.registerPrefix("UPLOAD$", standardDataPrefix + "/uploads")
  }

  // static<hostname_of_master>
  // We just connect to a standing spark cluster, no resize support.
  private val staticPattern = "static<(.+)>".r

  // standingGCE<name_of_cluster>
  // We just connect to an already initiated spark cluster running on Google Compute Engine.
  // Supports resizing.
  private val standingGCEPattern = "standingGCE<(.+)>".r

  // newGCE<name_of_cluster>
  // We need to create a new spark cluster running on Google Compute Engine.
  // Supports resizing.
  private val newGCEPattern = "newGCE<(.+)>".r

  lazy val BigGraphProductionEnvironment: BigGraphEnvironment = {

    // Make sure play and spark logs contain the proper context.
    val ctx = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val frameworkPackages = ctx.getFrameworkPackages
    frameworkPackages.add("play.api.Logger")
    frameworkPackages.add("org.apache.spark.Logging")

    val repoDirs =
      scala.util.Properties.envOrNone("REPOSITORY_MODE") match {
        case Some(staticRepoPattern(metaDir, dataDir, "")) =>
          new RepositoryDirs(metaDir, dataDir, standardDataPrefix)
        case Some(staticRepoPattern(metaDir, dataDir, ephemeralDataDir)) =>
          new RepositoryDirs(metaDir, dataDir, standardDataPrefix, Some(ephemeralDataDir))
        case _ =>
          throw new AssertionError("REPOSITORY_MODE is not defined")
      }
    repoDirs.forcePrefixRegistration()
    registerStandardPrefixes()

    scala.util.Properties.envOrElse("SPARK_CLUSTER_MODE", "static<local>") match {
      case staticPattern(master) =>
        new StaticSparkContextProvider() with StaticDirEnvironment {
          val repositoryDirs = repoDirs
        }
      case standingGCEPattern(clusterName) =>
        new spark_util.GCEManagedCluster(clusterName, "LynxKite", true) with StaticDirEnvironment {
          val repositoryDirs = repoDirs
        }
      case newGCEPattern(clusterName) =>
        new spark_util.GCEManagedCluster(clusterName, "LynxKite", false) with StaticDirEnvironment {
          val repositoryDirs = repoDirs
        }
    }
  }
}
