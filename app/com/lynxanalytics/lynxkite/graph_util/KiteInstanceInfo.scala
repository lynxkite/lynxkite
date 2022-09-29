// This is where all instance-specific configurations are collected.

package com.lynxanalytics.lynxkite.graph_util

object KiteInstanceInfo {
  val sparkVersion = org.apache.spark.SPARK_VERSION
  lazy val kiteVersion =
    try {
      scala.io.Source.fromFile(util.Properties.userDir + "/version").mkString
    } catch {
      case e: java.io.IOException => ""
    }
}
