// Copies data directory from ephemeral directory to persistent directory.
package com.lynxanalytics.biggraph.controllers

import org.apache.hadoop
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_util.{ HadoopFile, LoggedEnvironment }
import com.lynxanalytics.biggraph.serving

case class BackupSettings(
  dataDir: String = "",
  emphemeralDataDir: String = "",
  s3MetadataBucket: String = "",
  metadataVersionTimestamp: String = "")

case class BackupRequest(s3MetadataDir: String)

class CopyController(environment: BigGraphEnvironment, sparkClusterController: SparkClusterController) {
  private def lsRec(root: HadoopFile): Seq[HadoopFile] = {
    val fs = root.fs
    def ls(dirs: Seq[hadoop.fs.Path]): Stream[hadoop.fs.Path] = {
      if (dirs.isEmpty) Stream.empty
      else {
        val statuses = fs.listStatus(dirs.toArray)
        val (ds, files) = statuses.partition(s => s.isDirectory)
        files.map(_.getPath).toStream #::: ls(ds.map(_.getPath))
      }
    }
    ls(Seq(root.path)).map {
      file => root.hadoopFileForGlobOutput(file.toString)
    }
  }

  def getBackupSettings(user: serving.User, req: serving.Empty): BackupSettings = {
    import java.util.Calendar
    import java.text.SimpleDateFormat
    val ts = new SimpleDateFormat("YYYYMMddHHmmss").format(Calendar.getInstance().getTime())
    val dataDirPath = environment.dataManager.repositoryPath.resolvedName
    var ephemeralDataDirPath = ""
    for (ep <- environment.dataManager.ephemeralPath) {
      ephemeralDataDirPath = ep.resolvedName
    }
    BackupSettings(
      dataDir = dataDirPath,
      emphemeralDataDir = ephemeralDataDirPath,
      s3MetadataBucket = LoggedEnvironment.envOrElse("KITE_S3_METADATA_BUCKET", ""),
      metadataVersionTimestamp = ts)
  }

  def s3Backup(user: serving.User, req: BackupRequest): Unit = {
    println()
    println(req.s3MetadataDir)
    //val dm = environment.dataManager
    //dm.waitAllFutures()
    //dm.synchronized {
    //copyEphemeral(user, serving.Empty())
    //copyMetadata(user,req)
    //}
  }

  private def copyMetadata(user: serving.User, req: serving.Empty): Unit = {
    val dm = environment.dataManager
    dm.waitAllFutures()
    dm.synchronized {
      dm.waitAllFutures() // We don't want other operations to be running during copyMetadata.
      // Health checks would fail anyways because we are locking too long on dataManager here.
      // So we turn them off temporarily.
      sparkClusterController.setForceReportHealthy(true)
      try {
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
          val rdd = rc.sparkContext.parallelize(copies)
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
      } finally {
        sparkClusterController.setForceReportHealthy(false)
      }
    }

  }

  def copyEphemeral(user: serving.User, req: serving.Empty): Unit = {
    val dm = environment.dataManager
    dm.waitAllFutures()
    dm.synchronized {
      dm.waitAllFutures() // We don't want other operations to be running during s3copy.
      // Health checks would fail anyways because we are locking too long on dataManager here.
      // So we turn them off temporarily.
      sparkClusterController.setForceReportHealthy(true)
      try {
        for (ephemeralPath <- dm.ephemeralPath) {
          println("Path of backup destination:")
          println(ephemeralPath.toString)
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
          val rdd = rc.sparkContext.parallelize(copies)
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
      } finally {
        sparkClusterController.setForceReportHealthy(false)
      }
    }
  }
}
