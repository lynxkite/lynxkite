// Convenient Hadoop file interface.
package com.lynxanalytics.biggraph.graph_util

import com.esotericsoftware.kryo
import org.apache.hadoop
import org.apache.spark
import java.io.BufferedReader
import java.io.InputStreamReader
import com.lynxanalytics.biggraph.{Environment, logger => log}
import com.lynxanalytics.biggraph.serving.AccessControl
import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.spark_util._
import com.lynxanalytics.biggraph.partitioned_parquet._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.rdd.RDD
import scala.reflect.runtime.universe._

class HadoopFileSystemCache(val maxAllowedFileSystemLifeSpanMs: Long) {

  case class FileSystemWithExpiry(fileSystem: org.apache.hadoop.fs.FileSystem, expiry: Long) {
    def expired() = expiry < System.currentTimeMillis()
  }

  case class Key(scheme: String, authority: String)

  private val fileSystemCache =
    new scala.collection.mutable.HashMap[Key, FileSystemWithExpiry]().withDefaultValue(FileSystemWithExpiry(null, 0))

  def fs(owner: HadoopFile): org.apache.hadoop.fs.FileSystem = {
    def normalize(s: String) = {
      if (s != null) s.toLowerCase
      else ""
    }
    val key = Key(normalize(owner.uri.getScheme), normalize(owner.uri.getAuthority))
    fileSystemCache.synchronized {
      var current = fileSystemCache(key)
      if (current.expired()) {
        val fileSys = hadoop.fs.FileSystem.get(owner.uri, owner.hadoopConfiguration)
        val expires = System.currentTimeMillis() + maxAllowedFileSystemLifeSpanMs
        current = FileSystemWithExpiry(fileSys, expires)
        fileSystemCache(key) = current
      }
      current.fileSystem
    }
  }
}

object HadoopFile {

  private def hasDangerousEnd(str: String) =
    str.nonEmpty && !str.endsWith("@") && !str.endsWith("/")

  private def hasDangerousStart(str: String) =
    str.nonEmpty && !str.startsWith("/")

  def apply(
      str: String): HadoopFile = {
    val (prefixSymbol, relativePath) = PrefixRepository.splitSymbolicPattern(str)
    val prefixResolution = PrefixRepository.getPrefixInfo(prefixSymbol)
    val normalizedFullPath = PathNormalizer.normalize(prefixResolution + relativePath)
    assert(
      normalizedFullPath.startsWith(prefixResolution),
      s"$str is not inside $prefixSymbol")
    val normalizedRelativePath = normalizedFullPath.drop(prefixResolution.length)
    assert(
      !hasDangerousEnd(prefixResolution) || !hasDangerousStart(relativePath),
      s"The path following $prefixSymbol has to start with a slash (/)")
    new HadoopFile(prefixSymbol, normalizedRelativePath)
  }

  lazy val defaultFs = hadoop.fs.FileSystem.get(spark.SparkHacks.conf)
  private val s3nWithCredentialsPattern = "(s3[na]?)://(.+):(.+)@(.+)".r
  private val s3nNoCredentialsPattern = "(s3[na]?)://(.+)".r

  private val cache = new HadoopFileSystemCache(
    Environment.envOrElse("KITE_MAX_ALLOWED_FILESYSTEM_LIFESPAN_MS", (1000L * 3600 * 12).toString).toLong)

  def fs(hadoopFile: HadoopFile) = {
    cache.fs(hadoopFile)
  }
}

class HadoopFile private (
    val prefixSymbol: String,
    val normalizedRelativePath: String)
    extends Serializable with AccessControl {

  override def equals(o: Any) = o match {
    case o: HadoopFile =>
      o.prefixSymbol == this.prefixSymbol && o.normalizedRelativePath == this.normalizedRelativePath
    case _ => false
  }
  override def hashCode = (prefixSymbol + normalizedRelativePath).hashCode

  val symbolicName = prefixSymbol + normalizedRelativePath
  val resolvedName = PrefixRepository.getPrefixInfo(prefixSymbol) + normalizedRelativePath

  val (scheme, resolvedNameWithNoCredentials, awsId, awsSecret) = resolvedName match {
    case HadoopFile.s3nWithCredentialsPattern(scheme, key, secret, relPath) =>
      (scheme, scheme + "://" + relPath, key, secret)
    case HadoopFile.s3nNoCredentialsPattern(scheme, relPath) =>
      (scheme, scheme + "://" + relPath, "", "")
    case _ =>
      ("", resolvedName, "", "")
  }

  private def hasCredentials = awsId.nonEmpty

  override def toString = symbolicName

  def hadoopConfiguration(): hadoop.conf.Configuration = {
    val conf = spark.SparkHacks.conf
    conf.set(s"fs.${uri.getScheme}.impl.disable.cache", "true")
    if (hasCredentials) {
      scheme match {
        case "s3n" =>
          conf.set("fs.s3n.awsAccessKeyId", awsId)
          conf.set("fs.s3n.awsSecretAccessKey", awsSecret)
        case "s3" =>
          conf.set("fs.s3.awsAccessKeyId", awsId)
          conf.set("fs.s3.awsSecretAccessKey", awsSecret)
        case "s3a" =>
          conf.set("fs.s3a.access.key", awsId)
          conf.set("fs.s3a.secret.key", awsSecret)
      }
    }
    return conf
  }

  private def reinstateCredentialsIfNeeded(hadoopOutput: String): String = {
    if (hasCredentials) {
      hadoopOutput match {
        case HadoopFile.s3nNoCredentialsPattern(scheme, path) =>
          scheme + "://" + awsId + ":" + awsSecret + "@" + path
      }
    } else {
      hadoopOutput
    }
  }

  private def computeRelativePathFromHadoopOutput(hadoopOutput: String): String = {
    val hadoopOutputWithCredentials = reinstateCredentialsIfNeeded(hadoopOutput)
    val resolution = PrefixRepository.getPrefixInfo(prefixSymbol)
    assert(
      hadoopOutputWithCredentials.startsWith(resolution),
      s"Bad prefix match: $hadoopOutputWithCredentials ($hadoopOutput) should start with $resolution")
    hadoopOutputWithCredentials.drop(resolution.length)
  }

  // This function processes the paths returned by hadoop 'ls' (= the globStatus command)
  // after we called globStatus with this hadoop file.
  def hadoopFileForGlobOutput(hadoopOutput: String): HadoopFile = {
    new HadoopFile(prefixSymbol, computeRelativePathFromHadoopOutput(hadoopOutput))
  }

  def fs = HadoopFile.fs(this)
  @transient lazy val uri = path.toUri
  @transient lazy val path = new hadoop.fs.Path(resolvedNameWithNoCredentials)
  // The caller is responsible for calling close().
  def copyToLocalFile(dstPath: String) = fs.copyToLocalFile(false, path, new hadoop.fs.Path(dstPath), true)
  def copyFromLocalFile(srcPath: String) = fs.copyFromLocalFile(new hadoop.fs.Path(srcPath), path)
  def open() = fs.open(path)
  // The caller is responsible for calling close().
  def create() = fs.create(path)
  def createEmpty() = create().close()
  def exists() = fs.exists(path)
  private def reader() = new BufferedReader(new InputStreamReader(open, "utf-8"))
  def readAsString() = {
    val r = reader()
    try org.apache.commons.io.IOUtils.toString(r)
    finally r.close()
  }
  def readFirstLine() = {
    val r = reader()
    try r.readLine
    finally r.close()
  }
  def delete() = fs.delete(path, true)
  def deleteIfExists() = !exists() || delete()
  def renameTo(fn: HadoopFile) = fs.rename(path, fn.path)
  // globStatus() returns null instead of an empty array when there are no matches.
  private def globStatus: Array[hadoop.fs.FileStatus] =
    Option(fs.globStatus(path)).getOrElse(Array())
  def list = globStatus.map(st => hadoopFileForGlobOutput(st.getPath.toString))
  def name = path.getName()
  def length = fs.getFileStatus(path).getLen
  def globLength = globStatus.map(_.getLen).sum

  def listStatus = fs.listStatus(path)
  def getContentSummary = fs.getContentSummary(path)

  def loadTextFile(sc: spark.SparkContext): spark.rdd.RDD[String] = {
    val conf = hadoopConfiguration
    // Make sure we get many small splits.
    conf.setLong("mapred.max.split.size", 50000000)
    sc.newAPIHadoopFile(
      resolvedNameWithNoCredentials,
      kClass = classOf[hadoop.io.LongWritable],
      vClass = classOf[hadoop.io.Text],
      fClass = classOf[hadoop.mapreduce.lib.input.TextInputFormat],
      conf = conf,
    )
      .map(pair => pair._2.toString)
  }

  def saveAsTextFile(lines: spark.rdd.RDD[String]): Unit = {
    // RDD.saveAsTextFile does not take a hadoop.conf.Configuration argument. So we struggle a bit.
    val hadoopLines = lines.map(x => (hadoop.io.NullWritable.get(), new hadoop.io.Text(x)))
    hadoopLines.saveAsNewAPIHadoopFile(
      resolvedNameWithNoCredentials,
      keyClass = classOf[hadoop.io.NullWritable],
      valueClass = classOf[hadoop.io.Text],
      outputFormatClass = classOf[hadoop.mapreduce.lib.output.TextOutputFormat[hadoop.io.NullWritable, hadoop.io.Text]],
      conf = new hadoop.mapred.JobConf(hadoopConfiguration),
    )
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
    val myKryo = BigGraphSparkContext.createKryoWithForcedRegistration()
    val output = new kryo.io.Output(create())
    try {
      myKryo.writeClassAndObject(output, obj)
    } finally {
      output.close()
    }
  }

  def loadObjectKryo: Any = {
    val input = new kryo.io.Input(open())
    val res =
      try {
        RDDUtils.threadLocalKryo.get.readClassAndObject(input)
      } finally {
        input.close()
      }
    res
  }

  def mkdirs(): Unit = {
    fs.mkdirs(path)
  }

  // Loads an entity from Parquet, without deserializing it.
  def loadEntityRawDF(ss: spark.sql.SparkSession, numPartitions: Int): spark.sql.DataFrame = {
    ss.read.option("partitions", numPartitions).format(PartitionedParquet.format).load(this.resolvedName)
  }

  // Loads a Long-keyed rdd with deserialized values
  def loadEntityRDD[T: TypeTag](
      sc: spark.SparkContext,
      serializer: String,
      numPartitions: Int): RDD[(Long, T)] = {
    val ss = spark.sql.SparkSession.builder.config(sc.getConf).getOrCreate()
    val df = loadEntityRawDF(ss, numPartitions)
    val deserializer = graph_api.io.EntityDeserializer.forName[T](serializer)
    val rdd = deserializer.deserialize(df)
    assert(rdd.getNumPartitions == numPartitions)
    rdd
  }

  // Saves a DataFrame.
  def saveEntityRawDF(data: spark.sql.DataFrame): Unit = {
    if (fs.exists(path)) {
      log.info(s"deleting $path as it already exists (possibly as a result of a failed stage)")
      fs.delete(path, true)
    }
    log.info(s"saving entity data to ${symbolicName}")
    data.write.parquet(resolvedName)
  }

  // Saves a Long-keyed RDD, and returns the number of lines written and the serialization format.
  def saveEntityRDD[T](data: RDD[(Long, T)], tt: TypeTag[T]): (Long, String) = {
    val serializer = graph_api.io.EntitySerializer.forType(tt)
    implicit val ct = graph_api.RuntimeSafeCastable.classTagFromTypeTag(tt)
    val count = data.context.longAccumulator("row count")
    val counted = data.map(e => { count.add(1); e })
    val df = serializer.serialize(counted)
    saveEntityRawDF(df)
    (count.value, serializer.name)
  }

  def +(suffix: String): HadoopFile = {
    HadoopFile(symbolicName + suffix)
  }

  def /(path_element: String): HadoopFile = {
    this + ("/" + path_element)
  }

  def readAllowedFrom(user: com.lynxanalytics.biggraph.serving.User): Boolean = {
    val acl = PrefixRepository.getReadACL(prefixSymbol)
    aclContains(acl, user)
  }

  def writeAllowedFrom(user: com.lynxanalytics.biggraph.serving.User): Boolean = {
    val acl = PrefixRepository.getWriteACL(prefixSymbol)
    aclContains(acl, user)
  }

}

// A SequenceFile loader that creates one partition per file.
private[graph_util] class WholeSequenceFileInputFormat[K, V]
    extends hadoop.mapreduce.lib.input.SequenceFileInputFormat[K, V] {

  // Do not allow splitting/combining files.
  override protected def isSplitable(
      context: hadoop.mapreduce.JobContext,
      file: hadoop.fs.Path): Boolean = false

  // Read files in order.
  override protected def listStatus(
      job: hadoop.mapreduce.JobContext): java.util.List[hadoop.fs.FileStatus] = {
    val l = super.listStatus(job)
    java.util.Collections.sort(
      l,
      new java.util.Comparator[hadoop.fs.FileStatus] {
        def compare(a: hadoop.fs.FileStatus, b: hadoop.fs.FileStatus) =
          a.getPath.getName compare b.getPath.getName
      })
    l
  }
}
