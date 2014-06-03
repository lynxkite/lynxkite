package com.lynxanalytics.biggraph.graph_util

import org.apache.hadoop
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import java.io.BufferedReader
import java.io.InputStreamReader

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
  def fs() = hadoop.fs.FileSystem.get(uri, hadoopConfiguration)
  def uri() = new java.net.URI(filename)
  def path() = new hadoop.fs.Path(filename)
  def open() = fs.open(path)
  def reader() = new BufferedReader(new InputStreamReader(open))

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  /*
  def textFile(path: String, minSplits: Int = sc.defaultMinSplits): RDD[String] = {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minSplits).map(pair => pair._2.toString)
  }*/

  // TODO: set default splits?
  def loadAsTextFile(sc: spark.SparkContext): spark.rdd.RDD[String] = {
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

  def addSuffix(suffix: String): Filename = {
    Filename(filename + suffix, awsAccessKeyId, awsSecretAccessKey)
  }
}
