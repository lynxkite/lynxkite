// BigGraphEnvironment creates and holds the MetaGraphManager and DataManager.
package com.lynxanalytics.lynxkite

import org.apache.spark
import scala.concurrent._
import scala.concurrent.duration.Duration

import com.lynxanalytics.lynxkite.graph_util.HadoopFile
import com.lynxanalytics.lynxkite.graph_util.PrefixRepository

trait SparkSessionProvider {
  def createSparkSession: spark.sql.SparkSession
}

class StaticSparkSessionProvider() extends SparkSessionProvider {
  def createSparkSession = {
    logger.info("Initializing Spark...")
    val sparkSession = spark_util.BigGraphSparkContext.getSession("LynxKite")
    if (!sparkSession.sparkContext.isLocal) {
      logger.info("Wait 10 seconds for the workers to log in to the master...")
      Thread.sleep(10000)
    }
    logger.info("Spark initialized.")
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
  def sparkSession: spark.sql.SparkSession
  def sparkContext: spark.SparkContext
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
    val domainPreference = Environment.envOrElse("KITE_DOMAINS", "sphynx,spark,scala")
      .split(",").map(_.trim.toLowerCase)
    // Load the metagraph in parallel to Spark initialization.
    val metaGraphManagerFuture = Future(createMetaGraphManager(repositoryDirs))
    val domains = domainPreference.flatMap {
      case "spark" => Seq(createSparkDomain(sparkSessionProvider, repositoryDirs))
      case "sphynx" =>
        val host = Environment.envOrError("SPHYNX_HOST", "must be set when using Sphynx.")
        val port = Environment.envOrError("SPHYNX_PORT", "must be set when using Sphynx.")
        val certDir = Environment.envOrElse("SPHYNX_CERT_DIR", "")
        val unorderedDir = Environment.envOrError("UNORDERED_SPHYNX_DATA_DIR", "must be set when using Sphynx.")
        Seq(
          new graph_api.OrderedSphynxDisk(host, port.toInt, certDir),
          new graph_api.SphynxMemory(host, port.toInt, certDir),
          new graph_api.UnorderedSphynxLocalDisk(host, port.toInt, certDir, unorderedDir),
          new graph_api.UnorderedSphynxSparkDisk(host, port.toInt, certDir, repositoryDirs.dataDir / "sphynx"),
        )
      case "scala" => Seq(new graph_api.ScalaDomain)
      case other => throw new AssertionError(s"Unrecognized domain in KITE_DOMAINS: $other")
    }
    new BigGraphEnvironmentImpl(
      Await.result(metaGraphManagerFuture, Duration.Inf),
      new graph_api.DataManager(domains))
  }

  def createMetaGraphManager(repositoryDirs: RepositoryDirs) = {
    logger.info("Initializing meta graph manager...")
    val res = graph_api.MetaRepositoryManager(repositoryDirs.metaDir)
    logger.info("Meta graph manager initialized.")
    res
  }

  def createSparkDomain(sparkSessionProvider: SparkSessionProvider, repositoryDirs: RepositoryDirs) = {
    val sparkSession = sparkSessionProvider.createSparkSession
    logger.info("Initializing Spark domain...")
    val res = new graph_api.SparkDomain(
      sparkSession,
      repositoryDirs.dataDir,
      repositoryDirs.ephemeralDataDir)
    logger.info("Spark domain initialized.")
    res
  }

}

case class BigGraphEnvironmentImpl(
    metaGraphManager: graph_api.MetaGraphManager,
    dataManager: graph_api.DataManager)
    extends BigGraphEnvironment {
  // Try not to use these. If you need Spark, use it in an operation.
  def sparkDomain = dataManager.domains.collect { case d: graph_api.SparkDomain => d }.head
  def sparkSession = sparkDomain.sparkSession
  def sparkContext = sparkSession.sparkContext
}

class RepositoryDirs(
    val metaDir: String,
    dataDirSymbolicName: String,
    dataDirResolvedName: String,
    ephemeralDirResolvedName: Option[String] = None) {

  lazy val dataDir: HadoopFile = {
    PrefixRepository.registerPrefix(dataDirSymbolicName, dataDirResolvedName)
    val dir = HadoopFile(dataDirSymbolicName)
    logger.info(s"dataDir: ${dir.resolvedNameWithNoCredentials}")
    dir
  }

  lazy val ephemeralDataDir: Option[HadoopFile] = {
    ephemeralDirResolvedName.map {
      ephemeralDirResolvedName =>
        val ephemeralDirSymbolicName = "EPHEMERAL_" + dataDirSymbolicName
        PrefixRepository.registerPrefix(ephemeralDirSymbolicName, ephemeralDirResolvedName)
        val dir = HadoopFile(ephemeralDirSymbolicName)
        logger.info(s"ephemeralDataDir: ${dir.resolvedNameWithNoCredentials}")
        dir
    }
  }

  def forcePrefixRegistration(): Unit = {
    dataDir
    ephemeralDataDir
  }
}
