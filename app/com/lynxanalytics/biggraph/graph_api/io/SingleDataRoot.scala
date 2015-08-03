// The DataRoot is a directory where the DataManager stores data files.

package com.lynxanalytics.biggraph.graph_api.io

import java.util.UUID
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_util.HadoopFile

trait DataRoot {
  def /(p: String) = HadoopFileLike(this, Seq(p))
  def /(p: Seq[String]) = HadoopFileLike(this, p)
  def fastExists(path: Seq[String]): Boolean // May be out of date or incorrectly true.
  def forReading(path: Seq[String]): HadoopFile
  def forWriting(path: Seq[String]): HadoopFile
  def list(path: Seq[String]): Seq[HadoopFile]
}
case class HadoopFileLike(root: DataRoot, path: Seq[String]) {
  def /(p: String) = HadoopFileLike(root, path :+ p)
  def fastExists = root.fastExists(path)
  def forReading = root.forReading(path)
  def forWriting = root.forWriting(path)
  def list = root.list(path)
  def exists = fastExists && forReading.exists
}

class SingleDataRoot(repositoryPath: HadoopFile) extends DataRoot {
  // Contents of the top-level directories are cached.
  val contents = collection.mutable.Map[String, Set[String]]()
  def fastExists(path: Seq[String]) = {
    if (path.size < 2) true // Being incorrectly true is okay.
    else synchronized {
      val files = contents.getOrElseUpdate(path.head,
        (repositoryPath / path(0) / "*").list
          .map(_.path.getName)
          .toSet)
      files.contains(path(1))
    }
  }

  def forReading(path: Seq[String]) = forWriting(path)
  def forWriting(path: Seq[String]) = repositoryPath / path.mkString("/")
  def list(path: Seq[String]) = forReading(path).list
}

// All writes go to "a".
class CombinedRoot(a: SingleDataRoot, b: SingleDataRoot) extends DataRoot {
  def fastExists(path: Seq[String]) = a.fastExists(path) || b.fastExists(path)
  def forReading(path: Seq[String]) =
    if ((a / path).exists || !(b / path).exists) a.forReading(path) else b.forReading(path)
  def forWriting(path: Seq[String]) = a.forWriting(path)
  def list(path: Seq[String]) = (a / path).list ++ (b / path).list
}
