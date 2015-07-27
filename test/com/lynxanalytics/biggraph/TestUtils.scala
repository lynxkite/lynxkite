package com.lynxanalytics.biggraph

import java.io.File
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import org.apache.spark
import org.scalatest.Tag

import spark_util.BigGraphSparkContext

import scala.util.Random

object Benchmark extends Tag("Benchmark")

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
  val myTempDir = new File(
    "%s/%s-%d".format(sysTempDir, getClass.getName, scala.compat.Platform.currentTime))
  myTempDir.mkdir

  val myTempDirPrefix = TestUtils.getDummyPrefixName(myTempDir.toString)
  def tempDir(dirName: String): File = new File(myTempDir, dirName)
}

private object SparkContextContainer {
  lazy val sparkContext = BigGraphSparkContext(
    "BigGraphTests",
    useJars = false,
    debugKryo = false, // Set this to true if you are debugging kryo issues.
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
