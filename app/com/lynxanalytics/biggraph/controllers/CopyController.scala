// Copies data directory from ephemeral directory to persistent directory.
package com.lynxanalytics.biggraph.controllers

import org.apache.hadoop
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.serving

class CopyController(environment: BigGraphEnvironment) {

  // isDir is deprecated in Hadoop 2, isDirectory is not present in Hadoop 1.
  // Suppressing deprecation warning per SI-7934.
  @deprecated("", "") class Deprecated { def isDir(s: hadoop.fs.FileStatus) = s.isDir }
  object Deprecated extends Deprecated

  private def lsRec(root: HadoopFile): Seq[HadoopFile] = {
    val fs = root.fs
    def ls(dirs: Seq[hadoop.fs.Path]): Stream[hadoop.fs.Path] = {
      if (dirs.isEmpty) Stream.empty
      else {
        val statuses = fs.listStatus(dirs.toArray)
        val (ds, files) = statuses.partition(s => Deprecated.isDir(s))
        files.map(_.getPath).toStream #::: ls(ds.map(_.getPath))
      }
    }
    ls(Seq(root.path)).map {
      file => root.hadoopFileForGlobOutput(file.toString)
    }
  }

  def copyEphemeral(user: serving.User, req: serving.Empty): Unit = {
    val dm = environment.dataManager
    for (ephemeralPath <- dm.ephemeralPath) {
      log.info(s"Listing contents of $ephemeralPath...")
      val srcFiles = lsRec(ephemeralPath)
      val copies = srcFiles.map { src =>
        val relative = {
          assert(src.symbolicName.startsWith(ephemeralPath.symbolicName),
            s"$src is not in $ephemeralPath")
          src.symbolicName.drop(ephemeralPath.symbolicName.size)
        }
        val dst = dm.repositoryPath + relative
        src -> dst
      }
      log.info(s"Copying ${copies.size} files from $ephemeralPath to ${dm.repositoryPath}...")
      val rc = dm.runtimeContext
      val rdd = rc.sparkContext.parallelize(copies, rc.numAvailableCores)
      rdd.foreach {
        case (src, dst) =>
          hadoop.fs.FileUtil.copy(
            src.fs, src.path,
            dst.fs, dst.path,
            /* deleteSource = */ false, /* overwrite = */ true,
            dst.hadoopConfiguration)
      }
      log.info(s"Copied ${copies.size} files.")
    }
  }
}
