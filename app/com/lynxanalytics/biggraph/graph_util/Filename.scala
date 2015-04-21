// Convenient Hadoop file interface.
package com.lynxanalytics.biggraph.graph_util

import com.esotericsoftware.kryo
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.IOException

import com.lynxanalytics.biggraph.bigGraphLogger
import com.lynxanalytics.biggraph.spark_util.BigGraphSparkContext
import com.lynxanalytics.biggraph.spark_util.RDDUtils

import scala.util.Random

object SandboxedPath {
  private val sandboxedPathPattern = "([$][A-Z]+)(.*)".r
  private val pathResolutions = scala.collection.mutable.Map[String, String]()

  private def rootSymbolSyntaxIsOK(rootSymbol: String): Boolean = {
    rootSymbol match {
      case sandboxedPathPattern(_, _) => true
      case _ => false
    }
  }

  private def resolvePathRecursively(path: String): String = {
    path match {
      case sandboxedPathPattern(rootSymbol, rest) =>
        resolvePathRecursively(pathResolutions(rootSymbol)) + rest
      case _ => path
    }
  }

  def registerRoot(rootSymbol: String, rootResolution: String) = {
    assert(!pathResolutions.contains(rootSymbol), s"Root symbol $rootSymbol already set")
    assert(rootSymbolSyntaxIsOK(rootSymbol), s"Invalid root symbol syntax: $rootSymbol")
    val path = resolvePathRecursively(rootResolution)
    pathResolutions += rootSymbol -> path
  }

  def apply(str: String): SandboxedPath = str match {
    case sandboxedPathPattern(rootSymbol, relativePath) =>
      new SandboxedPath(rootSymbol, pathResolutions(rootSymbol), relativePath)
  }
  def fromAbsoluteToSymbolic(absolutePath: String, rootSymbol: String): SandboxedPath = {
    println(s"fromAbsoluteToSymbolic: path: [$absolutePath]  symbol: [$rootSymbol]")
    val rootResolution = pathResolutions(rootSymbol)
    assert(absolutePath.startsWith(rootResolution), s"Bad prefix match: $absolutePath should begin with $rootResolution")
    val r = absolutePath.replaceFirst(rootResolution, java.util.regex.Matcher.quoteReplacement(rootSymbol))
    //    println(s"back: absolute: $absolutePath, root: [$rootSymbol=$rootResolution] repl: $r")
    SandboxedPath(r)
  }

  // For testing
  private def randomRootName = "$" + Random.nextString(20).map(x => ((x % 26) + 'A').toChar)
  def getDummyRootName(rootPath: String): String = {
    val name = randomRootName
    if (rootPath.startsWith("file:"))
      registerRoot(name, rootPath)
    else
      registerRoot(name, "file:" + rootPath)
    name
  }

}

case class SandboxedPath(rootSymbol: String, rootResolution: String, relativePath: String) {

  def +(suffix: String): SandboxedPath = {
    this.copy(relativePath = relativePath + suffix)
  }
  def symbolicName = rootSymbol + relativePath
  def resolvedName = rootResolution + relativePath
}

case class Filename(sandboxedPath: SandboxedPath) {
  override def toString = sandboxedPath.symbolicName
  def fullString = toString
  def hadoopConfiguration(): hadoop.conf.Configuration = {
    val conf = new hadoop.conf.Configuration()
    conf.set("fs.s3n.awsAccessKeyId", "") // TODO These will come from someplace global
    conf.set("fs.s3n.awsSecretAccessKey", "") // TODO ditto
    return conf
  }
  @transient lazy val fs = hadoop.fs.FileSystem.get(uri, hadoopConfiguration)
  @transient lazy val uri = path.toUri
  @transient lazy val path = new hadoop.fs.Path(sandboxedPath.resolvedName)
  def open() = fs.open(path)
  def create() = fs.create(path)
  def exists() = fs.exists(path)
  def reader() = new BufferedReader(new InputStreamReader(open))
  def readAsString() = {
    val r = reader()
    Stream.continually(r.readLine()).takeWhile(_ != null).mkString
  }
  def delete() = fs.delete(path, true)
  def renameTo(fn: Filename) = fs.rename(path, fn.path)
  // globStatus() returns null instead of an empty array when there are no matches.
  private def globStatus = Option(fs.globStatus(path)).getOrElse(Array())
  def list = {
    import org.apache.hadoop.fs.Path
    println(s"sym: ${sandboxedPath.symbolicName}\nres: ${sandboxedPath.resolvedName}")
    println(s"uri: ${fs.getUri}")
    val p = new Path("/home")
    val q = fs.makeQualified(p)
    println(s"qua: $q")
    globStatus.map(st => this.copy(sandboxedPath = SandboxedPath.fromAbsoluteToSymbolic(st.getPath.toString, sandboxedPath.rootSymbol)))
  }

  def length = fs.getFileStatus(path).getLen
  def globLength = globStatus.map(_.getLen).sum

  def loadTextFile(sc: spark.SparkContext): spark.rdd.RDD[String] = {
    val conf = hadoopConfiguration
    // Make sure we get many small splits.
    conf.setLong("mapred.max.split.size", 50000000)
    sc.newAPIHadoopFile(
      sandboxedPath.resolvedName,
      kClass = classOf[hadoop.io.LongWritable],
      vClass = classOf[hadoop.io.Text],
      fClass = classOf[hadoop.mapreduce.lib.input.TextInputFormat],
      conf = conf)
      .map(pair => pair._2.toString)
  }

  def saveAsTextFile(lines: spark.rdd.RDD[String]): Unit = {
    // RDD.saveAsTextFile does not take a hadoop.conf.Configuration argument. So we struggle a bit.
    val hadoopLines = lines.map(x => (hadoop.io.NullWritable.get(), new hadoop.io.Text(x)))
    hadoopLines.saveAsHadoopFile(
      sandboxedPath.resolvedName,
      keyClass = classOf[hadoop.io.NullWritable],
      valueClass = classOf[hadoop.io.Text],
      outputFormatClass = classOf[hadoop.mapred.TextOutputFormat[hadoop.io.NullWritable, hadoop.io.Text]],
      conf = new hadoop.mapred.JobConf(hadoopConfiguration))
  }

  def createFromStrings(contents: String): Unit = {
    val stream = create()
    try {
      stream.write(contents.getBytes("UTF-8"))
    } finally {
      stream.close()
    }
  }

  def createFromObjectKryo(obj: Any): Unit = {
    /* It is not clear why, but createFromObjectKryo seems to gravely spoil the kryo instance
     * for any future deserialization operation. Basically trying to reuse a kryo instance for
     * deserialization will be orders of magnitude slower than a normal one.
     * We need to understand/fix this decently, but for now this is a stop-gap for the demo.
     * Looks like this might be fixable with a kryo upgrade.
     */
    val myKryo = BigGraphSparkContext.createKryo()
    val output = new kryo.io.Output(create())
    try {
      myKryo.writeClassAndObject(output, obj)
    } finally {
      output.close()
    }
  }

  def loadObjectKryo: Any = {
    val input = new kryo.io.Input(open())
    val res = try {
      RDDUtils.threadLocalKryo.get.readClassAndObject(input)
    } finally {
      input.close()
    }
    res
  }

  def mkdirs(): Unit = {
    fs.mkdirs(path)
  }

  def loadObjectFile[T: scala.reflect.ClassTag](sc: spark.SparkContext): spark.rdd.RDD[T] = {
    import hadoop.mapreduce.lib.input.SequenceFileInputFormat

    sc.newAPIHadoopFile(
      sandboxedPath.resolvedName,
      kClass = classOf[hadoop.io.NullWritable],
      vClass = classOf[hadoop.io.BytesWritable],
      fClass = classOf[SequenceFileInputFormat[hadoop.io.NullWritable, hadoop.io.BytesWritable]],
      conf = hadoopConfiguration)
      .map(pair => RDDUtils.kryoDeserialize[T](pair._2.getBytes))
  }

  def saveAsObjectFile(data: spark.rdd.RDD[_]): Unit = {
    import hadoop.mapreduce.lib.output.SequenceFileOutputFormat

    val hadoopData = data.map(x =>
      (hadoop.io.NullWritable.get(), new hadoop.io.BytesWritable(RDDUtils.kryoSerialize(x))))
    if (fs.exists(path)) {
      bigGraphLogger.info(s"deleting $path as it already exists (possibly as a result of a failed stage)")
      fs.delete(path, true)
    }
    bigGraphLogger.info(s"saving ${data.name} as object file to $sandboxedPath")
    hadoopData.saveAsNewAPIHadoopFile(
      sandboxedPath.resolvedName,
      keyClass = classOf[hadoop.io.NullWritable],
      valueClass = classOf[hadoop.io.BytesWritable],
      outputFormatClass =
        classOf[SequenceFileOutputFormat[hadoop.io.NullWritable, hadoop.io.BytesWritable]],
      conf = new hadoop.mapred.JobConf(hadoopConfiguration))
  }

  def +(suffix: String): Filename = {
    this.copy(sandboxedPath = sandboxedPath + suffix)
  }

  def /(path_element: String): Filename = {
    this + ("/" + path_element)
  }
}

object Filename {
  def apply(str: String): Filename = {
    new Filename(SandboxedPath(str))
  }
}
