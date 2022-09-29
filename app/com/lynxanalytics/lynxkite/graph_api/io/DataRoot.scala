// The DataRoot is a directory where the SparkDomain stores data files.

package com.lynxanalytics.lynxkite.graph_api.io

import com.lynxanalytics.lynxkite.graph_util.HadoopFile

trait DataRoot {
  def /(p: String) = HadoopFileLike(this, Seq(p))
  def /(p: Seq[String]) = HadoopFileLike(this, p)
  def mayHaveExisted(path: Seq[String]): Boolean // May be out of date or incorrectly true.
  def forReading(path: Seq[String]): HadoopFile
  def forWriting(path: Seq[String]): HadoopFile
  def list(path: Seq[String]): Seq[HadoopFile]
  def clear()
}
case class HadoopFileLike(root: DataRoot, path: Seq[String]) {
  def /(p: String) = HadoopFileLike(root, path :+ p)
  def mayHaveExisted = root.mayHaveExisted(path)
  def forReading = root.forReading(path)
  def forWriting = root.forWriting(path)
  def list = root.list(path)
  def exists = forReading.exists
}

class SingleDataRoot(repositoryPath: HadoopFile) extends DataRoot {
  // Contents of the top-level directories are cached.
  val contents = collection.mutable.Map[String, Set[String]]()

  // Preload directories we are sure are needed (to avoid post-initialization slowness).
  refresh()

  def refresh() = {
    filesInTopDir(ScalarsDir)
    filesInTopDir(EntitiesDir)
    filesInTopDir(TablesDir)
    filesInTopDir(PartitionedDir)
    filesInTopDir(OperationsDir)
  }

  def filesInTopDir(topDirName: String): Set[String] = {
    contents.getOrElseUpdate(
      topDirName,
      (repositoryPath / topDirName / "*").list
        .map(_.path.getName)
        .toSet)
  }

  def mayHaveExisted(path: Seq[String]) = {
    if (path.size < 2) true // Being incorrectly true is okay.
    else synchronized {
      val files = filesInTopDir(path.head)
      files.contains(path(1))
    }
  }

  // To force the re-read of top-level directories after a cleaner run.
  def clear() = {
    contents.clear()
    refresh()
  }

  def forReading(path: Seq[String]) = forWriting(path)
  def forWriting(path: Seq[String]) = repositoryPath / path.mkString("/")
  def list(path: Seq[String]) = forReading(path).list
}

// All writes go to "a".
class CombinedRoot(a: SingleDataRoot, b: SingleDataRoot) extends DataRoot {
  def mayHaveExisted(path: Seq[String]) = a.mayHaveExisted(path) || b.mayHaveExisted(path)
  def forReading(path: Seq[String]) =
    if ((a / path).exists || !(b / path).exists) a.forReading(path) else b.forReading(path)
  def forWriting(path: Seq[String]) = a.forWriting(path)
  def list(path: Seq[String]) = (a / path).list ++ (b / path).list
  def clear() = {
    a.clear()
    b.clear()
  }
}
