// Various spark benchmarks to be run from groovy scripts.

package com.lynxanalytics.biggraph.groovy

import org.apache.spark

class SparkTestListener(sc: spark.SparkContext, s: String) extends spark.scheduler.SparkListener {
  val localityInfo = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
  override def onStageCompleted(
    stageCompleted: spark.scheduler.SparkListenerStageCompleted): Unit = synchronized {
    val info = stageCompleted.stageInfo
    println(s"$s: STAGE DONE: [${info.stageId}.${info.attemptId}] localities: $localityInfo  " +
      s"${info.name} ${(info.completionTime.get - info.submissionTime.get) / 1000} seconds")
    localityInfo.clear()
  }
  override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
    val locInfo = taskEnd.taskInfo.taskLocality.toString
    localityInfo(locInfo) += 1
  }
}

class SparkTests(sc: spark.SparkContext) {

  def cacheTest(params: java.util.Map[String, AnyRef]) {
    val storageLevelString = params
      .get("storageLevel")
      .asInstanceOf[String]
    val storageLevel =
      if (storageLevelString == null) null
      else spark.storage.StorageLevel.fromString(storageLevelString)
    val numVertices = params
      .get("dataSize")
      .asInstanceOf[String]
      .toInt
    val numPartitions = params
      .get("numPartitions")
      .asInstanceOf[String]
      .toInt

    val listener = new SparkTestListener(sc, storageLevelString)
    sc.addSparkListener(listener)

    val vertices = sc.parallelize(0 to (numVertices - 1), numPartitions)
    val data = vertices
      .mapPartitionsWithIndex {
        (pidx, it) =>
          val r = new scala.util.Random(pidx)
          it.map { _ =>
            (r.nextInt(numVertices).toLong, (r.nextLong, r.nextLong))
          }
      }
      .partitionBy(new spark.HashPartitioner(numPartitions))

    if (storageLevel != null) {
      data.persist(storageLevel)
    }

    data.foreach(identity)
    data.foreach(identity)
  }

}
