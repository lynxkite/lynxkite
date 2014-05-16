package com.lynxanalytics.biggraph

import java.io.File
import org.apache.spark

import com.lynxanalytics.biggraph.spark_util.BigGraphSparkContext

trait BigGraphEnviroment {
  val sparkContext: spark.SparkContext
  val bigGraphManager: graph_api.BigGraphManager
  val graphDataManager: graph_api.GraphDataManager
}

/*
 * This object is used to initialize common state shared by multiple controllers.
 * TODO: figure out what is the best practice for this in play.
 */
object BigGraphProductionEnviroment extends BigGraphEnviroment {
  lazy val sparkContext = BigGraphSparkContext(
      "BigGraphServer",
      scala.util.Properties.envOrElse("SPARK_MASTER", "local"))

  private val sysTempDir = System.getProperty("java.io.tmpdir")
  private val myTempDir = new File(
      "%s/%s-%d".format(sysTempDir, getClass.getName, scala.compat.Platform.currentTime))
  myTempDir.mkdir
  private val graphDir = new File(myTempDir, "graph")
  graphDir.mkdir
  private val dataDir = new File(myTempDir, "data")
  dataDir.mkdir

  lazy val bigGraphManager = graph_api.BigGraphManager(graphDir.toString)
  lazy val graphDataManager = graph_api.GraphDataManager(sparkContext, dataDir.toString)
}
