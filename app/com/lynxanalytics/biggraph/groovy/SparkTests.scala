// Varius spark benchmarks to be run from groovy scripts.

package com.lynxanalytics.biggraph.groovy

import org.apache.spark

class SparkTests(sc: spark.SparkContext) {

  def iterativeTest(params: java.util.Map[String, AnyRef]) {
    val storageLevelString = params
      .get("storageLevel")
      .asInstanceOf[String]
    val storageLevel = if (storageLevelString == null) null else spark.storage.StorageLevel.fromString(storageLevelString)
    val numVertices = params
      .get("dataSize")
      .asInstanceOf[String]
      .toInt
    val numPartitions = 100
    val vertices = sc.parallelize(0 to (numVertices - 1), numPartitions)
    val data = vertices
      .map {
        id =>
          val r = new scala.util.Random(id)
          (r.nextInt(numVertices), Unit)
      }
      .partitionBy(new spark.HashPartitioner(numPartitions))

    /*      .flatMap {
        case (id, cnt) =>
          val r = new scala.util.Random(id)
          (1 to 20).map {
            _ =>
              (r.nextInt(numVertices), cnt)
          }
      }
      .reduceByKey(_ + _)*/

    if (storageLevel != null) {
      data.persist(storageLevel)
    }

    data.foreach(identity)
    data.foreach(identity)

  }

}
