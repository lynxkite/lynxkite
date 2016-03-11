// This is where all instance-specific configurations are collected.
// We also dump this info to the logfile (both at creating the file
// and at renaming it.

package com.lynxanalytics.biggraph.graph_util

import ch.qos.logback.core.status.{ Status, StatusListener }
import play.api.libs.json
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object KiteInstanceInfo {
  private val marker = "KITE_INFO_MARKER"
  val sparkVersion = org.apache.spark.SPARK_VERSION
  lazy val kiteVersion = try {
    scala.io.Source.fromFile(util.Properties.userDir + "/version").mkString
  } catch {
    case e: java.io.IOException => ""
  }
  val js = json.Json.obj(
    "kiteVersion" -> kiteVersion,
    "sparkVersion" -> sparkVersion
  )
  def dump(): Unit = synchronized {
    log.info(s"$marker $js")
  }
}

class FileRollingListener extends StatusListener {
  override def addStatusEvent(status: Status): Unit = {
    val packageName = status.getOrigin.getClass.getPackage.getName
    if (packageName == "ch.qos.logback.core.rolling")
      Future { // We can't do the log in this callstack, since we're already in a log call
        // and logback implements recursion protection.
        KiteInstanceInfo.dump()
      }
  }
}
