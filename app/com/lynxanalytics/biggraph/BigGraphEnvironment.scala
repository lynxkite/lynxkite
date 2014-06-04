package com.lynxanalytics.biggraph

import java.io.File
import org.apache.spark

import graph_util.Filename

trait SparkContextProvider {
  val sparkContext: spark.SparkContext

  def allowsClusterResize: Boolean = false
  def numInstances: Int = ???
  def setNumInstances(numInstances: Int): Unit = ???
}

class StaticSparkContextProvider(master: String) extends SparkContextProvider {
  val sparkContext = spark_util.BigGraphSparkContext("BigGraphServer", master)
}

trait BigGraphEnvironment extends SparkContextProvider {
  val bigGraphManager: graph_api.BigGraphManager
  val graphDataManager: graph_api.GraphDataManager
}

trait StaticDirEnvironment extends BigGraphEnvironment {
  val repositoryDirs: RepositoryDirs

  // For now metadata can only be saved locally.
  override lazy val bigGraphManager = graph_api.BigGraphManager(repositoryDirs.graphDir)
  override lazy val graphDataManager = graph_api.GraphDataManager(
    sparkContext, repositoryDirs.dataDir)
}

trait RepositoryDirs {
  val graphDir: String
  val dataDir: Filename
}
class TemporaryRepositoryDirs extends RepositoryDirs {
  private val sysTempDir = System.getProperty("java.io.tmpdir")
  private val myTempDir = new File(
    "%s/%s-%d".format(sysTempDir, getClass.getName, scala.compat.Platform.currentTime))
  myTempDir.mkdir
  private val graphDirFile = new File(myTempDir, "graph")
  graphDirFile.mkdir
  private val dataDirFile = new File(myTempDir, "data")
  dataDirFile.mkdir

  val graphDir = graphDirFile.toString
  val dataDir = Filename(dataDirFile.toString)
}
