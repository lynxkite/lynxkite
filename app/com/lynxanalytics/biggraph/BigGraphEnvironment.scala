package com.lynxanalytics.biggraph

import java.io.File
import org.apache.spark

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

trait TemporaryDirEnvironment extends BigGraphEnvironment {
  private val sysTempDir = System.getProperty("java.io.tmpdir")
  private val myTempDir = new File(
    "%s/%s-%d".format(sysTempDir, getClass.getName, scala.compat.Platform.currentTime))
  myTempDir.mkdir
  private val graphDir = new File(myTempDir, "graph")
  graphDir.mkdir
  private val dataDir = new File(myTempDir, "data")
  dataDir.mkdir

  override lazy val bigGraphManager = graph_api.BigGraphManager(graphDir.toString)
  override lazy val graphDataManager = graph_api.GraphDataManager(sparkContext, dataDir.toString)
}
