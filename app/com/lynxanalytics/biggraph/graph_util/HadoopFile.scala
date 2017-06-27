// Convenient Hadoop file interface.
package com.lynxanalytics.biggraph.graph_util

import com.esotericsoftware.kryo
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.deploy.SparkHadoopUtil
import java.io.BufferedReader
import java.io.InputStreamReader
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.serving.AccessControl
import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.spark_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.rdd.RDD
import scala.reflect.runtime.universe._

object HadoopFile {

  private def hasDangerousEnd(str: String) =
    str.nonEmpty && !str.endsWith("@") && !str.endsWith("/")

  private def hasDangerousStart(str: String) =
    str.nonEmpty && !str.startsWith("/")

  def apply(
    str: String,
    legacyMode: Boolean = false,
    parentLazyFS: Option[LazySharedFileSystem] = None): HadoopFile = {
    val (prefixSymbol, relativePath) = PrefixRepository.splitSymbolicPattern(str, legacyMode)
    val prefixResolution = PrefixRepository.getPrefixInfo(prefixSymbol)
    val normalizedFullPath = PathNormalizer.normalize(prefixResolution + relativePath)
    assert(normalizedFullPath.startsWith(prefixResolution),
      s"$str is not inside $prefixSymbol")
    val normalizedRelativePath = normalizedFullPath.drop(prefixResolution.length)
    assert(!hasDangerousEnd(prefixResolution) || !hasDangerousStart(relativePath),
      s"The path following $prefixSymbol has to start with a slash (/)")
    new HadoopFile(prefixSymbol, normalizedRelativePath, parentLazyFS)
  }

  lazy val defaultFs = hadoop.fs.FileSystem.get(SparkHadoopUtil.get.conf)
  private val s3nWithCredentialsPattern = "(s3n?)://(.+):(.+)@(.+)".r
  private val s3nNoCredentialsPattern = "(s3n?)://(.+)".r

  // We want to avoid creating a FileSystem object for each HadoopFile. But we also don't want to
  // create the FileSystem object unnecessarily. Both desires are fulfilled by LazySharedFileSystem.
  class LazySharedFileSystem(f: HadoopFile) extends Serializable {
    @transient lazy val get = hadoop.fs.FileSystem.get(f.uri, f.hadoopConfiguration)
  }
}

class HadoopFile private (
    val prefixSymbol: String,
    val normalizedRelativePath: String,
    parentLazyFS: Option[HadoopFile.LazySharedFileSystem]) extends Serializable with AccessControl {

  override def equals(o: Any) = o match {
    case o: HadoopFile =>
      o.prefixSymbol == this.prefixSymbol && o.normalizedRelativePath == this.normalizedRelativePath
    case _ => false
  }
  override def hashCode = (prefixSymbol + normalizedRelativePath).hashCode

  val symbolicName = prefixSymbol + normalizedRelativePath
  val resolvedName = PrefixRepository.getPrefixInfo(prefixSymbol) + normalizedRelativePath

  val (resolvedNameWithNoCredentials, awsId, awsSecret) = resolvedName match {
    case HadoopFile.s3nWithCredentialsPattern(scheme, key, secret, relPath) =>
      (scheme + "://" + relPath, key, secret)
    case _ =>
      (resolvedName, "", "")
  }

  private def hasCredentials = awsId.nonEmpty

  override def toString = symbolicName

  def hadoopConfiguration(): hadoop.conf.Configuration = {
    val conf = SparkHadoopUtil.get.conf
    if (hasCredentials) {
      conf.set("fs.s3n.awsAccessKeyId", awsId)
      conf.set("fs.s3n.awsSecretAccessKey", awsSecret)
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
    assert(hadoopOutputWithCredentials.startsWith(resolution),
      s"Bad prefix match: $hadoopOutputWithCredentials ($hadoopOutput) should start with $resolution")
    hadoopOutputWithCredentials.drop(resolution.length)
  }

  // This function processes the paths returned by hadoop 'ls' (= the globStatus command)
  // after we called globStatus with this hadoop file.
  def hadoopFileForGlobOutput(hadoopOutput: String): HadoopFile = {
    new HadoopFile(prefixSymbol, computeRelativePathFromHadoopOutput(hadoopOutput), lazyFSOpt)
  }

  val lazyFSOpt =
    if (resolvedName.isEmpty) None // We cannot have a FileSystem because we don't have a path.
    else parentLazyFS.orElse(Some(new HadoopFile.LazySharedFileSystem(this)))
  def fs = lazyFSOpt.get.get
  @transient lazy val uri = path.toUri
  @transient lazy val path = new hadoop.fs.Path(resolvedNameWithNoCredentials)
  // The caller is responsible for calling close().
  def open() = fs.open(path)
  // The caller is responsible for calling close().
  def create() = fs.create(path)
  def exists() = fs.exists(path)
  private def reader() = new BufferedReader(new InputStreamReader(open, "utf-8"))
  def readAsString() = {
    val r = reader()
    try org.apache.commons.io.IOUtils.toString(r) finally r.close()
  }
  def readFirstLine() = {
    val r = reader()
    try r.readLine finally r.close()
  }
  def delete() = fs.delete(path, true)
  def deleteIfExists() = !exists() || delete()
  def renameTo(fn: HadoopFile) = fs.rename(path, fn.path)
  // globStatus() returns null instead of an empty array when there are no matches.
  private def globStatus: Array[hadoop.fs.FileStatus] =
    Option(fs.globStatus(path)).getOrElse(Array())
  def list = globStatus.map(st => hadoopFileForGlobOutput(st.getPath.toString))

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
      conf = conf)
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

  // Loads a Long-keyed SortedRDD, optionally with a specific partitioner.
  // This can load the legacy format (see issue #2018).
  // Note that this method returns a sortedRDD, as opposed to the
  // new load methods, which return simple RDDs
  def loadLegacyEntityRDD[T: scala.reflect.ClassTag](
    sc: spark.SparkContext,
    partitioner: Option[spark.Partitioner] = None): SortedRDD[Long, T] = {

    val file = sc.newAPIHadoopFile(
      resolvedNameWithNoCredentials,
      kClass = classOf[hadoop.io.NullWritable],
      vClass = classOf[hadoop.io.BytesWritable],
      fClass = classOf[WholeSequenceFileInputFormat[hadoop.io.NullWritable, hadoop.io.BytesWritable]],
      conf = hadoopConfiguration)
    val p = partitioner.getOrElse(new spark.HashPartitioner(file.partitions.size))
    file
      .map { case (k, v) => RDDUtils.kryoDeserialize[(Long, T)](v.getBytes) }
      .asUniqueSortedRDD(p)
  }

  // Loads a Long-keyed rdd where the values are just raw bytes
  def loadEntityRawRDD(sc: spark.SparkContext): RDD[(Long, hadoop.io.BytesWritable)] = {
    val file = sc.newAPIHadoopFile(
      resolvedNameWithNoCredentials,
      kClass = classOf[hadoop.io.LongWritable],
      vClass = classOf[hadoop.io.BytesWritable],
      fClass = classOf[WholeSequenceFileInputFormat[hadoop.io.LongWritable, hadoop.io.BytesWritable]],
      conf = hadoopConfiguration)
    file.map { case (k, v) => k.get -> v }
  }

  // Loads a Long-keyed rdd with deserialized values
  def loadEntityRDD[T: TypeTag](sc: spark.SparkContext, serializer: String): RDD[(Long, T)] = {
    val raw = loadEntityRawRDD(sc)
    val deserializer = graph_api.io.EntityDeserializer.forName[T](serializer)
    raw.mapValues(deserializer.deserialize(_))
  }

  // Saves a Long-keyed rdd where the values are just raw bytes;
  // Returns the number of lines written
  def saveEntityRawRDD(data: RDD[(Long, hadoop.io.BytesWritable)]): Long = {
    import hadoop.mapreduce.lib.output.SequenceFileOutputFormat

    val lines = data.context.longAccumulator("Line count")
    val hadoopData = data.map {
      case (k, v) =>
        lines.add(1)
        new hadoop.io.LongWritable(k) -> v
    }
    if (fs.exists(path)) {
      log.info(s"deleting $path as it already exists (possibly as a result of a failed stage)")
      fs.delete(path, true)
    }
    log.info(s"saving ${data.name} as object file to ${symbolicName}")
    hadoopData.saveAsNewAPIHadoopFile(
      resolvedNameWithNoCredentials,
      keyClass = classOf[hadoop.io.LongWritable],
      valueClass = classOf[hadoop.io.BytesWritable],
      outputFormatClass =
        classOf[SequenceFileOutputFormat[hadoop.io.LongWritable, hadoop.io.BytesWritable]],
      conf = new hadoop.mapred.JobConf(hadoopConfiguration))
    lines.value
  }

  // Saves a Long-keyed RDD, and returns the number of lines written and the serialization format.
  def saveEntityRDD[T](data: RDD[(Long, T)], tt: TypeTag[T]): (Long, String) = {
    val serializer = graph_api.io.EntitySerializer.forType(tt)
    implicit val ct = graph_api.RuntimeSafeCastable.classTagFromTypeTag(tt)
    val raw = data.mapValues(serializer.serialize(_))
    raw.setName(data.name)
    val lines = saveEntityRawRDD(raw)
    (lines, serializer.name)
  }

  def +(suffix: String): HadoopFile = {
    HadoopFile(symbolicName + suffix, parentLazyFS = lazyFSOpt)
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
    context: hadoop.mapreduce.JobContext, file: hadoop.fs.Path): Boolean = false

  // Read files in order.
  override protected def listStatus(
    job: hadoop.mapreduce.JobContext): java.util.List[hadoop.fs.FileStatus] = {
    val l = super.listStatus(job)
    java.util.Collections.sort(l, new java.util.Comparator[hadoop.fs.FileStatus] {
      def compare(a: hadoop.fs.FileStatus, b: hadoop.fs.FileStatus) =
        a.getPath.getName compare b.getPath.getName
    })
    l
  }
}
