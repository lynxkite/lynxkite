// Hack to access some private methods.
package org.apache.spark

object EntityIOHelper extends mapreduce.SparkHadoopMapReduceUtil {
  def createTaskAttemptContext(
    config: org.apache.hadoop.conf.Configuration,
    jobtracker: String,
    stage: Int,
    isMap: Boolean,
    partition: Int,
    attempt: Int) = {
    val attemptID = newTaskAttemptID(jobtracker, stage, isMap, partition, attempt)
    newTaskAttemptContext(config, attemptID)
  }
}
