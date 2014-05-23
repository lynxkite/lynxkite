package com.lynxanalytics.biggraph

import java.io.File
import org.apache.spark

import com.lynxanalytics.biggraph.spark_util.BigGraphSparkContext
import com.lynxanalytics.biggraph.spark_util.ManagedSparkCluster

trait BigGraphEnviroment {
  def sparkContext: spark.SparkContext
  val bigGraphManager: graph_api.BigGraphManager
  def graphDataManager: graph_api.GraphDataManager
}

/*
 * This object is used to initialize common state shared by multiple controllers.
 * TODO: figure out what is the best practice for this in play.
 */
object BigGraphProductionEnviroment extends BigGraphEnviroment {
  val applicationName = "BigGraphServer"

  def sparkContext: spark.SparkContext = synchronized {
      if (localMode) localSparkContext
      else clusterManager.sparkContext
    }

  lazy val bigGraphManager = graph_api.BigGraphManager(graphDir.toString)
  def graphDataManager = synchronized { internalGraphDataManager}

  // 0 means local spark context.
  def useInstances(numInstances: Int) = synchronized {
      if (localMode) {
        if (numInstances > 0) {
          newRemoteMode
          clusterManager.setNumInstances(numInstances)
        }
      } else {
        if (numInstances == 0) {
          newLocalMode
        } else {
          clusterManager.setNumInstances(numInstances)
        }
      }
    }

  private var internalGraphDataManager: graph_api.GraphDataManager = null

  private def resetGraphDataManager: Unit = {
    internalGraphDataManager = graph_api.GraphDataManager(sparkContext, dataDir.toString)
  }

  private val sysTempDir = System.getProperty("java.io.tmpdir")
  private val myTempDir = new File(
      "%s/%s-%d".format(sysTempDir, getClass.getName, scala.compat.Platform.currentTime))
  myTempDir.mkdir
  private val graphDir = new File(myTempDir, "graph")
  graphDir.mkdir
  private val dataDir = new File(myTempDir, "data")
  dataDir.mkdir

  private val clusterManager = new ManagedSparkCluster(
      scala.util.Properties.envOrElse("SPARK_CLUSTER_NAME", "my-precious"),
      "BigGraphServer")

  private var localMode = true

  private var localSparkContext: spark.SparkContext = null

  private def killCurrrentContext: Unit = {
    if (localMode) {
      if (localSparkContext != null) {
        localSparkContext.stop
        localSparkContext = null
      }
    } else {
      clusterManager.disconnect
      clusterManager.shutDownCluster
    }
  }

  private def newLocalMode: Unit = {
    killCurrrentContext
    localSparkContext = BigGraphSparkContext(applicationName, "local")
    localMode = true
    resetGraphDataManager
  }

  private def newRemoteMode: Unit = {
    killCurrrentContext
    clusterManager.startUpCluster
    // TODO wait up for the cluster.
    clusterManager.connect
    localMode = false
    resetGraphDataManager
  }

  newLocalMode
}
