// This is where all instance-specific configurations are collected.

package com.lynxanalytics.biggraph.graph_util

object KiteInstanceInfo {
  lazy val kiteVersion =
    try {
      scala.io.Source.fromFile(util.Properties.userDir + "/version").mkString
    } catch {
      case e: java.io.IOException => ""
    }
}
