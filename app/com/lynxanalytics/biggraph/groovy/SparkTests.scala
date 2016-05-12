// Varius spark benchmarks to be run from groovy scripts.

package com.lynxanalytics.biggraph.groovy

import org.apache.spark

class SparkTestListener(sc: spark.SparkContext, s: String) extends spark.scheduler.SparkListener {
  override def onStageCompleted(
    stageCompleted: spark.scheduler.SparkListenerStageCompleted): Unit = synchronized {
    val info = stageCompleted.stageInfo
    println(
      s"$s: STAGE DONE: [${info.stageId}.${info.attemptId}] ${info.name} ${(info.completionTime.get - info.submissionTime.get) / 1000} seconds")
  }
}

class SparkTests(sc: spark.SparkContext) {

  def cacheTest(params: java.util.Map[String, AnyRef]) {
    val storageLevelString = params
      .get("storageLevel")
      .asInstanceOf[String]
    val storageLevel = if (storageLevelString == null) null else spark.storage.StorageLevel.fromString(storageLevelString)
    val numVertices = params
      .get("dataSize")
      .asInstanceOf[String]
      .toInt

    val listener = new SparkTestListener(sc, storageLevelString)
    sc.addSparkListener(listener)

    val numPartitions = 100
    val vertices = sc.parallelize(0 to (numVertices - 1), numPartitions)
    val data = vertices
      .mapPartitionsWithIndex {
        (pidx, it) =>
          val r = new scala.util.Random(pidx)
          it.map { _ => (r.nextInt(numVertices), Unit) }
      }
      .partitionBy(new spark.HashPartitioner(numPartitions))

    if (storageLevel != null) {
      data.persist(storageLevel)
    }

    data.foreach(identity)
    data.foreach(identity)

    //    System.in.read()
  }

}
