// Endpoints for downloading files.
package com.lynxanalytics.biggraph.serving

import java.io._
import scala.collection.JavaConversions._
import play.api.mvc
import play.api.libs.concurrent.Execution.Implicits._

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class DownloadFileRequest(
    path: String, // Symbolic path to directory.
    // If stripHeaders is true, the first lines will be stripped from all but the first file.
    stripHeaders: Boolean) {
  def name = path.split("/").last
}

object Downloads extends play.api.http.HeaderNames {

  // This is for old, non-DataFrame CSV exports that have a "header" file and a "data" directory.
  def oldCSVDownload(user: User, request: mvc.Request[mvc.AnyContent]) = {
    log.info(s"download: $user ${request.path}")
    val path = HadoopFile(request.getQueryString("path").get)
    val name = request.getQueryString("name").get
    val files = Seq(path / "header") ++ (path / "data" / "*").list
    val length = files.map(_.length).sum
    log.info(s"downloading $length bytes: $files")
    val stream = new SequenceInputStream(files.view.map(_.open).iterator)
    mvc.Result(
      header = mvc.ResponseHeader(200, Map(
        CONTENT_LENGTH -> length.toString,
        CONTENT_DISPOSITION -> s"attachment; filename=$name.csv")),
      body = play.api.libs.iteratee.Enumerator.fromStream(stream)
    )
  }

  // Downloads all the files in directory concatenated into one.
  def downloadFile(user: User, request: DownloadFileRequest) = {
    log.info(s"download: $user ${request.path}")
    val path = HadoopFile(request.path)
    assert((path / "_SUCCESS").exists, s"$path/_SUCCESS does not exist.")
    val files = (path / "part-*").list.sortBy(_.symbolicName)
    val length = files.map(_.length).sum
    log.info(s"downloading $length bytes: $files")
    val streams = {
      val streams = files.iterator.map(_.open)
      if (request.stripHeaders) new HeaderSkippingStreamIterator(request.path, streams)
      else streams
    }
    val stream = new SequenceInputStream(streams)
    mvc.Result(
      header = mvc.ResponseHeader(200, Map(
        CONTENT_LENGTH -> length.toString,
        CONTENT_DISPOSITION -> s"attachment; filename=${request.name}")),
      body = play.api.libs.iteratee.Enumerator.fromStream(stream)
    )
  }
}

// This is for reading multiple CSV files with headers as a single CSV file with a header.
// The first line of the first file is considered the header, and is stripped from subsequent files.
class HeaderSkippingStreamIterator(path: String, streams: Iterator[InputStream])
    extends Iterator[InputStream] {
  var header: Array[Byte] = null

  def hasNext = streams.hasNext

  def next = {
    val f = streams.next
    val firstLine = readLine(f)
    if (firstLine.length == 0) {
      new ByteArrayInputStream(Array()) // Skip empty file.
    } else if (header == null) { // First non-empty file.
      header = firstLine
      // Now we have to "put back" the header into the stream.
      new SequenceInputStream(Iterator(new ByteArrayInputStream(firstLine), f))
    } else {
      assert(
        java.util.Arrays.equals(firstLine, header),
        {
          val firstLineStr = new String(firstLine, "utf-8")
          val headerStr = new String(header, "utf-8")
          s"Unexpected first line ($firstLineStr) in $path. (Expected $headerStr.)"
        })
      f
    }
  }

  // Reads a line into bytes. Includes the newline (0x0a) at the end.
  def readLine(in: InputStream): Array[Byte] = {
    val bytes = new collection.mutable.ArrayBuffer[Byte]
    while (true) {
      val b = in.read()
      if (b == -1) return bytes.toArray
      bytes += b.toByte
      if (b == 0x0a) return bytes.toArray
    }
    ???
  }
}
