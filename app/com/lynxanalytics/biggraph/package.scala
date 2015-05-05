// Package-level variables. Creates our logger and the BigGraphEnvironment.
package com.lynxanalytics

import com.lynxanalytics.biggraph.graph_util.RootRepository
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._

package object biggraph {
  val bigGraphLogger = LoggerFactory.getLogger("BigGraph backend")

  // Initialize reflection to avoid thread-safety issues
  // TODO: ditch this when we get to Scala 2.11
  def printType[T: TypeTag]: Unit = bigGraphLogger.debug("initialize reflection for type: " + typeOf[T])

  printType[Long]
  printType[String]
  printType[Double]
  printType[Array[Long]]

  // static<big_graph_dir,graph_data_dir>
  private val staticRepoPattern = "static<(.+),(.+)>".r

  val standardDataPrefix = "DATA$"

  def registerStandardPrefixes() = {
    RootRepository.registerRoot("UPLOAD$", standardDataPrefix + "/uploads")
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

    val repoDirs =
      scala.util.Properties.envOrElse("REPOSITORY_MODE", "local_random") match {
        case staticRepoPattern(bigGraphDir, graphDataDir) =>
          new RegularRepositoryDirs(bigGraphDir, graphDataDir, standardDataPrefix)
        case "local_random" => new TemporaryRepositoryDirs(standardDataPrefix)
      }
    repoDirs.forcePrefixRegistration()
    registerStandardPrefixes()

    scala.util.Properties.envOrElse("SPARK_CLUSTER_MODE", "static<local>") match {
      case staticPattern(master) =>
        new StaticSparkContextProvider() with StaticDirEnvironment {
          val repositoryDirs = repoDirs
        }
      case standingGCEPattern(clusterName) =>
        new spark_util.GCEManagedCluster(clusterName, "BigGraphServer", true) with StaticDirEnvironment {
          val repositoryDirs = repoDirs
        }
      case newGCEPattern(clusterName) =>
        new spark_util.GCEManagedCluster(clusterName, "BigGraphServer", false) with StaticDirEnvironment {
          val repositoryDirs = repoDirs
        }
    }
  }
}
