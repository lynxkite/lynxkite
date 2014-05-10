package com.lynxanalytics.biggraph

import java.io.File
import org.apache.spark

object TestUtils {
  def RDDToSortedString(rdd: spark.rdd.RDD[_]): String = {
    rdd.collect.toSeq.map(_.toString).sorted.mkString("\n")
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
  lazy val sparkContext = new spark.SparkContext("local", "BigGraphTests")
}

trait TestSparkContext {
  val sparkContext = SparkContextContainer.sparkContext
}
