// Making some Spark internals available to LynxKite.
package org.apache.spark
import org.apache.spark.deploy.SparkHadoopUtil

object SparkHacks {
  val conf = SparkHadoopUtil.get.conf
}
