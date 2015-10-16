// BigGraphEnvironment creates and holds the MetaGraphManager and DataManager.
package com.lynxanalytics.biggraph

import java.io.File
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

trait BigGraphEnvironment {
  def sparkContext: spark.SparkContext
  def metaGraphManager: graph_api.MetaGraphManager
  def dataManager: graph_api.DataManager
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
    Await.ready(Future.sequence(Seq(
      metaGraphManagerFuture,
      sparkContextFuture,
      dataManagerFuture)),
      Duration.Inf)
    new BigGraphEnvironmentImpl(
      sparkContextFuture.value.get.get,
      metaGraphManagerFuture.value.get.get,
      dataManagerFuture.value.get.get)
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
