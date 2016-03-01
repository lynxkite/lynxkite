// BigGraphEnvironment creates and holds the MetaGraphManager and DataManager.
package com.lynxanalytics.biggraph

import org.apache.spark
import scala.concurrent._
import scala.concurrent.duration.Duration

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.PrefixRepository

trait SparkContextProvider {
  def createSparkContext: spark.SparkContext
}

class StaticSparkContextProvider() extends SparkContextProvider {
  def createSparkContext = {
    bigGraphLogger.info("Initializing Spark...")
    val sparkContext = spark_util.BigGraphSparkContext("LynxKite")
    if (!sparkContext.isLocal) {
      bigGraphLogger.info("Wait 10 seconds for the workers to log in to the master...")
      Thread.sleep(10000)
    }
    bigGraphLogger.info("Spark initialized.")
    sparkContext
  }
}

// An environment that does not allow its holder to do any actual spark computations,
// it only allows for meta level manipulations.
trait SparkFreeEnvironment {
  def metaGraphManager: graph_api.MetaGraphManager
  def entityProgressManager: graph_api.EntityProgressManager
}

trait BigGraphEnvironment extends SparkFreeEnvironment {
  def sparkContext: spark.SparkContext
  def metaGraphManager: graph_api.MetaGraphManager
  def dataManager: graph_api.DataManager
  def entityProgressManager = dataManager
  var watchdogEnabled = true

  private lazy val uniqueId = table.DefaultSource.register(this)
  def dataFrame: spark.sql.DataFrameReader = {
    dataManager.masterSQLContext.read
      .format("com.lynxanalytics.biggraph.table")
      .option("environment", uniqueId)
  }
}

object BigGraphEnvironmentImpl {
  def createStaticDirEnvironment(
    repositoryDirs: RepositoryDirs,
    sparkContextProvider: SparkContextProvider): BigGraphEnvironment = {

    import scala.concurrent.ExecutionContext.Implicits.global
    // Initialize parts of the environment. Some of them can be initialized
    // in parallel, hence the juggling with futures.
    val metaGraphManagerFuture = Future(createMetaGraphManager(repositoryDirs))
    val sparkContextFuture = Future(sparkContextProvider.createSparkContext)
    val dataManagerFuture = sparkContextFuture.map(
      sparkContext => createDataManager(sparkContext, repositoryDirs))
    val envFuture = for {
      sparkContext <- sparkContextFuture
      metaGraphManager <- metaGraphManagerFuture
      dataManager <- dataManagerFuture
    } yield new BigGraphEnvironmentImpl(sparkContext, metaGraphManager, dataManager)
    Await.result(envFuture, Duration.Inf)
  }

  def createMetaGraphManager(repositoryDirs: RepositoryDirs) = {
    bigGraphLogger.info("Initializing meta graph manager...")
    val res = graph_api.MetaRepositoryManager(repositoryDirs.metaDir)
    bigGraphLogger.info("Meta graph manager initialized.")
    res
  }

  def createDataManager(sparkContext: spark.SparkContext, repositoryDirs: RepositoryDirs) = {
    bigGraphLogger.info("Initializing data manager...")
    val res = new graph_api.DataManager(
      sparkContext, repositoryDirs.dataDir, repositoryDirs.ephemeralDataDir)
    bigGraphLogger.info("Data manager initialized.")
    res
  }

}

case class BigGraphEnvironmentImpl(
  sparkContext: spark.SparkContext,
  metaGraphManager: graph_api.MetaGraphManager,
  dataManager: graph_api.DataManager) extends BigGraphEnvironment

class RepositoryDirs(
    val metaDir: String,
    dataDirSymbolicName: String,
    dataDirResolvedName: String,
    ephemeralDirResolvedName: Option[String] = None) {

  lazy val dataDir: HadoopFile = {
    PrefixRepository.registerPrefix(dataDirSymbolicName, dataDirResolvedName)
    HadoopFile(dataDirSymbolicName)
  }

  lazy val ephemeralDataDir: Option[HadoopFile] = {
    ephemeralDirResolvedName.map {
      ephemeralDirResolvedName =>
        val ephemeralDirSymbolicName = "EPHEMERAL_" + dataDirSymbolicName
        PrefixRepository.registerPrefix(ephemeralDirSymbolicName, ephemeralDirResolvedName)
        HadoopFile(ephemeralDirSymbolicName)
    }
  }

  def forcePrefixRegistration(): Unit = {
    dataDir
    ephemeralDataDir
  }
}
