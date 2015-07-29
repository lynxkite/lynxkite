// BigGraphEnvironment creates and holds the MetaGraphManager and DataManager.
package com.lynxanalytics.biggraph

import java.io.File
import org.apache.spark

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.PrefixRepository

trait SparkContextProvider {
  val sparkContext: spark.SparkContext

  def allowsClusterResize: Boolean = false
  def numInstances: Int = ???
  def setNumInstances(numInstances: Int): Unit = ???
}

class StaticSparkContextProvider() extends SparkContextProvider {
  val sparkContext = spark_util.BigGraphSparkContext("LynxKite")
  if (!sparkContext.isLocal) {
    bigGraphLogger.info("Wait 10 seconds for the workers to log in to the master...")
    Thread.sleep(10000)
  }
}

trait BigGraphEnvironment extends SparkContextProvider {
  val metaGraphManager: graph_api.MetaGraphManager
  val dataManager: graph_api.DataManager
}

trait StaticDirEnvironment extends BigGraphEnvironment {
  val repositoryDirs: RepositoryDirs

  override lazy val metaGraphManager = graph_api.MetaRepositoryManager(repositoryDirs.graphDir)
  override lazy val dataManager = new graph_api.DataManager(
    sparkContext, repositoryDirs.dataDir, repositoryDirs.ephemeralDataDir)
}

trait RepositoryDirs {
  val graphDir: String
  val dataDirSymbolicName: String
  val dataDirResolvedName: String
  lazy val dataDir: HadoopFile = {
    PrefixRepository.registerPrefix(dataDirSymbolicName, dataDirResolvedName)
    HadoopFile(dataDirSymbolicName)
  }
  lazy val ephemeralDataDir: Option[HadoopFile] = None
  def forcePrefixRegistration(): Unit = {
    dataDir
    ephemeralDataDir
  }
}

class RegularRepositoryDirs(
  val graphDir: String,
  val dataDirResolvedName: String,
  val dataDirSymbolicName: String) extends RepositoryDirs

class RegularRepositoryDirsWithEphemeral(
    val graphDir: String,
    val dataDirResolvedName: String,
    val dataDirSymbolicName: String,
    ephemeralDirResolvedName: String) extends RepositoryDirs {

  val ephemeralDirSymbolicName = "EPHEMERAL_" + dataDirSymbolicName
  override lazy val ephemeralDataDir: Option[HadoopFile] = {
    PrefixRepository.registerPrefix(ephemeralDirSymbolicName, ephemeralDirResolvedName)
    Some(HadoopFile(ephemeralDirSymbolicName))
  }
}

class TemporaryRepositoryDirs(val dataDirSymbolicName: String) extends RepositoryDirs {
  private val sysTempDir = System.getProperty("java.io.tmpdir")
  private val myTempDir = new File(
    "%s/%s-%d".format(sysTempDir, getClass.getName, scala.compat.Platform.currentTime))
  myTempDir.mkdir
  private val graphDirFile = new File(myTempDir, "graph")
  graphDirFile.mkdir

  private val dataDirFile = new File(myTempDir, "data")
  dataDirFile.mkdir

  val graphDir = graphDirFile.toString
  val dataDirResolvedName = dataDirFile.toString
}
