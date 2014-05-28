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

  def saveAsTextFile(lines: spark.rdd.RDD[String]): Unit = {
    // RDD.saveAsTextFile does not take a hadoop.conf.Configuration argument. So we struggle a bit.
    val hadoopLines = lines.map(x => (hadoop.io.NullWritable.get(), new hadoop.io.Text(x)))
    hadoopLines.saveAsHadoopFile(
      filename,
      keyClass = classOf[hadoop.io.NullWritable],
      valueClass = classOf[hadoop.io.Text],
      outputFormatClass = classOf[hadoop.mapred.TextOutputFormat[hadoop.io.NullWritable, hadoop.io.Text]],
      conf = new hadoop.mapred.JobConf(hadoopConfiguration)
    )
  }

  def addSuffix(suffix: String): Filename = {
    Filename(filename + suffix, awsAccessKeyId, awsSecretAccessKey)
  }

  val fileSize: Long = fs.getFileStatus(path).getLen
}
