package com.lynxanalytics.biggraph.graph_util

import org.apache.hadoop
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.IOException

import com.lynxanalytics.biggraph.spark_util.RDDUtils

case class Filename(
    val filename: String,
    val awsAccessKeyId: String = "",
    val awsSecretAccessKey: String = "") {
  override def toString() = filename
  def hadoopConfiguration(): hadoop.conf.Configuration = {
    val conf = new hadoop.conf.Configuration()
    conf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    conf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)
    return conf
  }
  @transient lazy val fs = hadoop.fs.FileSystem.get(uri, hadoopConfiguration)
  @transient lazy val uri = new java.net.URI(filename)
  @transient lazy val path = new hadoop.fs.Path(filename)
  def open() = fs.open(path)
  def exists() = fs.exists(path)
  def reader() = new BufferedReader(new InputStreamReader(open))

  def loadTextFile(sc: spark.SparkContext): spark.rdd.RDD[String] = {
    // SparkContext.textfile does not accept hadoop configuration as a parameter (we need to pass AWS credentials)
    // textfile calls hadoopfile that uses MRv1 while the newAPIHadoopFile uses the MRv2 API (and accepts conf)
    sc.newAPIHadoopFile(
      filename,
      kClass = classOf[hadoop.io.LongWritable],
      vClass = classOf[hadoop.io.Text],
      fClass = classOf[hadoop.mapreduce.lib.input.TextInputFormat],
      conf = hadoopConfiguration)
      .map(pair => pair._2.toString)
  }

  def saveAsTextFile(lines: spark.rdd.RDD[String]): Unit = {
    // RDD.saveAsTextFile does not take a hadoop.conf.Configuration argument. So we struggle a bit.
    val hadoopLines = lines.map(x => (hadoop.io.NullWritable.get(), new hadoop.io.Text(x)))
    hadoopLines.saveAsHadoopFile(
      filename,
      keyClass = classOf[hadoop.io.NullWritable],
      valueClass = classOf[hadoop.io.Text],
      outputFormatClass = classOf[hadoop.mapred.TextOutputFormat[hadoop.io.NullWritable, hadoop.io.Text]],
      conf = new hadoop.mapred.JobConf(hadoopConfiguration))
  }

  def createFromStrings(contents: String): Unit = {
    val stream = fs.create(path)
    stream.write(contents.getBytes("UTF-8"))
    stream.close()
  }

  def makeDir(): Unit = {
    if (fs.exists(path)) throw new IOException("Directory already exists") else fs.mkdirs(path)
  }

  def loadObjectFile[T: scala.reflect.ClassTag](sc: spark.SparkContext): spark.rdd.RDD[T] = {
    import hadoop.mapreduce.lib.input.SequenceFileInputFormat

    sc.newAPIHadoopFile(
      filename,
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
    hadoopData.saveAsNewAPIHadoopFile(
      filename,
      keyClass = classOf[hadoop.io.NullWritable],
      valueClass = classOf[hadoop.io.BytesWritable],
      outputFormatClass =
        classOf[SequenceFileOutputFormat[hadoop.io.NullWritable, hadoop.io.BytesWritable]],
      conf = new hadoop.mapred.JobConf(hadoopConfiguration))
  }

  def addSuffix(suffix: String): Filename = {
    Filename(filename + suffix, awsAccessKeyId, awsSecretAccessKey)
  }

  def addPathElement(path_element: String): Filename = {
    addSuffix("/" + path_element)
  }
}
object Filename {
  private val filenamePattern = "(s3n?)://(.+):(.+)@(.+)".r
  def fromString(str: String): Filename = {
    str match {
      case filenamePattern(protocol, id, key, path) =>
        Filename(protocol + "://" + path, id, key)
      case _ => Filename(str)
    }
  }
}
