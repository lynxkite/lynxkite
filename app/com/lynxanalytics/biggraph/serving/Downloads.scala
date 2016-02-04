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

class HeaderSkippingStreamIterator(path: String, streams: Iterator[InputStream])
    extends Iterator[InputStream] {
  var header: String = null

  def hasNext = streams.hasNext

  def next = {
    val f = streams.next
    val firstLine = readLine(f)
    if (header == null) {
      header = firstLine
      // Now we have to "put back" the header into the stream.
      new SequenceInputStream(Iterator(
        new ByteArrayInputStream((firstLine + "\n").getBytes("utf-8")),
        f))
    } else {
      assert(
        firstLine == header,
        s"Unexpected first line ($firstLine) in $path. (Expected $header.)")
      f
    }
  }

  def readLine(in: InputStream): String = {
    val bytes = new collection.mutable.ArrayBuffer[Byte]
    while (true) {
      val b = in.read()
      if (b == -1 || b == 0x0a) {
        return new String(bytes.toArray, "utf-8")
      }
      bytes += b.toByte
    }
    ???
  }
}
