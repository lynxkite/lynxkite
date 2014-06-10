package com.lynxanalytics.biggraph

import java.io.File
import org.apache.spark

import spark_util.BigGraphSparkContext

object TestUtils {
  def RDDToSortedString(rdd: spark.rdd.RDD[_]): String = {
    rdd.collect.toSeq.map(_.toString).sorted.mkString("\n")
  }

  /*
   * Runs a shell command and returns its stdout as a string.
   */
  def runShellCommand(command: String): String = {
    import scala.sys.process._
    Seq("sh", "-c", command).!!
  }
}

trait TestTempDir {
  val sysTempDir = System.getProperty("java.io.tmpdir")
  val myTempDir = new File(
    "%s/%s-%d".format(sysTempDir, getClass.getName, scala.compat.Platform.currentTime))
  myTempDir.mkdir

  def tempDir(dirName: String): File = new File(myTempDir, dirName)
}

private object SparkContextContainer {
  lazy val sparkContext = BigGraphSparkContext(
    "BigGraphTests",
    "local",
    useJars = false,
    useKryo = false, // TODO(darabos): Figure out why Kryo gets stuck.
    debugKryo = false) // Set this to true if you are debugging kryo issues.
}

trait TestSparkContext {
  val sparkContext = SparkContextContainer.sparkContext
}
