// BigGraphEnvironment creates and holds the MetaGraphManager and DataManager.
package com.lynxanalytics.biggraph

import org.apache.spark
import scala.concurrent._
import scala.concurrent.duration.Duration

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment

trait SparkSessionProvider {
  def createSparkSession: spark.sql.SparkSession
}

class StaticSparkSessionProvider() extends SparkSessionProvider {
  def createSparkSession = {
    bigGraphLogger.info("Initializing Spark...")
    val sparkSession = spark_util.BigGraphSparkContext.getSession("LynxKite")
    if (!sparkSession.sparkContext.isLocal) {
      bigGraphLogger.info("Wait 10 seconds for the workers to log in to the master...")
      Thread.sleep(10000)
    }
    bigGraphLogger.info("Spark initialized.")
    sparkSession
  }
}

// An environment that does not allow its holder to do any actual spark computations,
// it only allows for meta level manipulations.
trait SparkFreeEnvironment {
  def metaGraphManager: graph_api.MetaGraphManager
  def entityProgressManager: graph_api.EntityProgressManager
}

trait BigGraphEnvironment extends SparkFreeEnvironment {
  val sparkSession: spark.sql.SparkSession
  val sparkContext: spark.SparkContext
  def metaGraphManager: graph_api.MetaGraphManager
  def dataManager: graph_api.DataManager
  def sparkDomain: graph_api.SparkDomain
  def entityProgressManager = dataManager
}

object BigGraphEnvironmentImpl {
  def createStaticDirEnvironment(
      repositoryDirs: RepositoryDirs,
      sparkSessionProvider: SparkSessionProvider): BigGraphEnvironment = {

    import scala.concurrent.ExecutionContext.Implicits.global
    val domainPreference = LoggedEnvironment.envOrElse("KITE_DOMAINS", "spark,sphynx,scala")
      .split(",").map(_.trim.toLowerCase)
    // Load the metagraph in parallel to Spark initialization.
    val metaGraphManagerFuture = Future(createMetaGraphManager(repositoryDirs))
    val sparkSession = sparkSessionProvider.createSparkSession
    val domains = domainPreference.flatMap {
      case "spark" => Seq(createSparkDomain(sparkSession, repositoryDirs))
      case "sphynx" =>
        val host = LoggedEnvironment.envOrError("SPHYNX_HOST", "must be set when using Sphynx.")
        val port = LoggedEnvironment.envOrError("SPHYNX_PORT", "must be set when using Sphynx.")
        val certDir = LoggedEnvironment.envOrError("SPHYNX_CERT_DIR", "must be set when using Sphynx.")
        val unorderedDir = LoggedEnvironment.envOrError("UNORDERED_SPHYNX_DATA_DIR", "must be set when using Sphynx.")
        Seq(
          new graph_api.OrderedSphynxDisk(host, port.toInt, certDir),
          new graph_api.SphynxMemory(host, port.toInt, certDir),
          new graph_api.UnorderedSphynxLocalDisk(host, port.toInt, certDir, unorderedDir),
          new graph_api.UnorderedSphynxSparkDisk(host, port.toInt, certDir, repositoryDirs.dataDir / "sphynx"),
        )
      case "scala" => Seq(new graph_api.ScalaDomain)
    }
    new BigGraphEnvironmentImpl(
      sparkSession,
      Await.result(metaGraphManagerFuture, Duration.Inf),
      domains.collect { case d: graph_api.SparkDomain => d }.head, // TODO: Remove it someday?
      new graph_api.DataManager(domains))
  }

  def createMetaGraphManager(repositoryDirs: RepositoryDirs) = {
    bigGraphLogger.info("Initializing meta graph manager...")
    val res = graph_api.MetaRepositoryManager(repositoryDirs.metaDir)
    bigGraphLogger.info("Meta graph manager initialized.")
    res
  }

  def createSparkDomain(sparkSession: spark.sql.SparkSession, repositoryDirs: RepositoryDirs) = {
    bigGraphLogger.info("Initializing data manager...")
    val res = new graph_api.SparkDomain(
      sparkSession,
      repositoryDirs.dataDir,
      repositoryDirs.ephemeralDataDir)
    bigGraphLogger.info("Data manager initialized.")
    res
  }

}

case class BigGraphEnvironmentImpl(
    sparkSession: spark.sql.SparkSession,
    metaGraphManager: graph_api.MetaGraphManager,
    sparkDomain: graph_api.SparkDomain,
    dataManager: graph_api.DataManager)
    extends BigGraphEnvironment {
  val sparkContext = sparkSession.sparkContext
}

class RepositoryDirs(
    val metaDir: String,
    dataDirSymbolicName: String,
    dataDirResolvedName: String,
    ephemeralDirResolvedName: Option[String] = None) {

  lazy val dataDir: HadoopFile = {
    PrefixRepository.registerPrefix(dataDirSymbolicName, dataDirResolvedName)
    val dir = HadoopFile(dataDirSymbolicName)
    bigGraphLogger.info(s"dataDir: ${dir.resolvedNameWithNoCredentials}")
    dir
  }

  lazy val ephemeralDataDir: Option[HadoopFile] = {
    ephemeralDirResolvedName.map {
      ephemeralDirResolvedName =>
        val ephemeralDirSymbolicName = "EPHEMERAL_" + dataDirSymbolicName
        PrefixRepository.registerPrefix(ephemeralDirSymbolicName, ephemeralDirResolvedName)
        val dir = HadoopFile(ephemeralDirSymbolicName)
        bigGraphLogger.info(s"ephemeralDataDir: ${dir.resolvedNameWithNoCredentials}")
        dir
    }
  }

  def forcePrefixRegistration(): Unit = {
    dataDir
    ephemeralDataDir
  }
}
