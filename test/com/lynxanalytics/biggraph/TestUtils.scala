package com.lynxanalytics.biggraph

import java.io.File
import com.lynxanalytics.biggraph.graph_api.io.EntityIO
import com.lynxanalytics.biggraph.graph_util.{ HadoopFile, PrefixRepository, Timestamp }
import org.apache.spark
import org.scalatest.Tag

import spark_util.BigGraphSparkContext

import scala.util.Random

object Benchmark extends Tag("Benchmark")

object TestUtils {
  def RDDToSortedString(rdd: spark.rdd.RDD[_]): String = {
    rdd.collect.toSeq.map(_.toString).sorted.mkString("\n")
  }

  def withRestoreGlobals[T](verticesPerPartition: Int, tolerance: Double)(fn: => T): T = {
    val savedVerticesPerPartition = EntityIO.verticesPerPartition
    val savedTolerance = EntityIO.tolerance
    try {
      EntityIO.verticesPerPartition = verticesPerPartition
      EntityIO.tolerance = tolerance
      fn
    } finally {
      EntityIO.verticesPerPartition = savedVerticesPerPartition
      EntityIO.tolerance = savedTolerance
    }
  }

  /*
   * Runs a shell command and returns its stdout as a string.
   */
  def runShellCommand(command: String): String = {
    import scala.sys.process._
    Seq("sh", "-c", command).!!
  }

  def randomPrefixSymbol = Random.nextString(20).map(x => ((x % 26) + 'A').toChar) + "$"
  def getDummyPrefixName(path: String, canSupplyFileScheme: Boolean = true): String = {
    val name = randomPrefixSymbol
    if (path.startsWith("/") && canSupplyFileScheme)
      PrefixRepository.registerPrefix(name, "file:" + path)
    else
      PrefixRepository.registerPrefix(name, path)
    name
  }

}

trait TestTempDir {
  val sysTempDir = System.getProperty("java.io.tmpdir")
  val myTempDir = new File(s"$sysTempDir/${getClass.getName}-$Timestamp")
  myTempDir.mkdir

  val myTempDirPrefix = TestUtils.getDummyPrefixName(myTempDir.toString)
  def tempDir(dirName: String): File = new File(myTempDir, dirName)

  // Used to create suitable repository paths for test DataManagers.
  def cleanDataManagerDir(): HadoopFile = {
    val managerDir = tempDir(s"dataManager.$Timestamp")
    managerDir.mkdir
    HadoopFile(TestUtils.getDummyPrefixName(managerDir.toString))
  }

  // Delete the temporary directory on exit.
  org.apache.commons.io.FileUtils.forceDeleteOnExit(myTempDir)
}

private object SparkContextContainer {
  lazy val sparkContext = BigGraphSparkContext(
    "BigGraphTests",
    forceRegistration = true,
    master = "local")
}

trait TestSparkContext {
  val sparkContext = SparkContextContainer.sparkContext
}

case class Timed[X](nanos: Long, value: X)
object Timed {
  def apply[X](f: => X): Timed[X] = {
    val t0 = System.nanoTime
    val value = f
    val duration = System.nanoTime - t0
    Timed(duration, value)
  }
}
