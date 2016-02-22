// Utilities to access application logs.

package com.lynxanalytics.biggraph.controllers

import java.io.File

import play.api.mvc
import play.api.libs.concurrent.Execution.Implicits._

import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class FileDescriptor(
  name: String,
  length: Long,
  lastModified: String)

case class LogFiles(files: List[FileDescriptor])

case class DownloadLogFileRequest(name: String)

class LogController extends play.api.http.HeaderNames {

  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  def getLogFiles(user: serving.User, req: serving.Empty): LogFiles = {
    assert(user.isAdmin, "Only admins can access the server logs")
    val logDir = new File(util.Properties.envOrElse("KITE_LOG_DIR", "logs"))
    assert(logDir.exists, s"Application log directory not found at $logDir")
    assert(logDir.isDirectory, s"$logDir is not a directory")
    val logFiles = logDir.listFiles
      .filter(_.isFile)
      // Sort log files in descending order of last modification date.
      .sortWith(_.lastModified > _.lastModified)
      .map { file =>
        FileDescriptor(
          file.getName,
          file.length,
          dateFormat.format(file.lastModified))
      }
    assert(logFiles.size > 0, "No application log file found")
    log.info(s"$user has downloaded the list of log files in $logDir")
    LogFiles(logFiles.toList)
  }

  def downloadLogFile(user: serving.User, request: DownloadLogFileRequest) = {
    assert(user.isAdmin, "Only admins can access the server logs")
    val logFile = new File(util.Properties.envOrElse("KITE_LOG_DIR", "logs"), request.name)
    assert(logFile.exists, s"Application log file not found at $logFile")
    log.info(s"$user has downloaded log file $logFile")
    mvc.Results.Ok.sendFile(logFile)
  }
}
