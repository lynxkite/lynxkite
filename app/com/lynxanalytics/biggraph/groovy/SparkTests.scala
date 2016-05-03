// Varius spark benchmarks to be run from groovy scripts.

package com.lynxanalytics.biggraph.groovy

import org.apache.spark

class SparkTests(sc: spark.SparkContext) {

  def iterativeTest(params: java.util.Map[String, AnyRef]) {
    val storageLevel = params
      .get("storageLevel")
      .asInstanceOf[spark.storage.StorageLevel]
    val numVertices = params
      .get("dataSize")
      .asInstanceOf[String]
      .toInt
    val vertices = sc.parallelize(0 to (numVertices - 1))

    var state = vertices.map(id => (id, 1L))
    var counters = vertices.map(id => 0)
    for (i <- 1 to 5) {
      if (storageLevel != null) {
        state.persist(storageLevel)
        state.foreach(identity) // force cache fill
      } else {
        state.foreach(identity) // do a forking (emulate real algo)
      }
      state = state
        .flatMap {
          case (id, cnt) =>
            val r = new scala.util.Random(id)
            (1 to 4).map {
              _ =>
                (r.nextInt(numVertices), cnt)
            }
        }
        .reduceByKey(_ + _)
    }

    state.foreach(identity)
  }

}
