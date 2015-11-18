// Hack to access some private methods.
package org.apache.spark

object ImportGraphHelper extends mapreduce.SparkHadoopMapReduceUtil {
  def createTaskAttemptContext(
    config: org.apache.hadoop.conf.Configuration,
    jobtracker: String,
    stage: Int,
    partition: Int,
    attempt: Int) = {
    val attemptID = newTaskAttemptID(jobtracker, stage, isMap = false, partition, attempt)
    newTaskAttemptContext(config, attemptID)
  }
}
