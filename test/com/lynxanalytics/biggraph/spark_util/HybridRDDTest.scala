package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.TestSparkContext

class HybridRDDTest extends FunSuite with TestSparkContext {
  test("lookup operations work as expected") {
    val rnd = new util.Random(0)
    val localSource = (0 until 1000).map(_ => (rnd.nextInt(100), rnd.nextLong()))
    val localLookup = (0 until 50).map(x => (x, rnd.nextDouble()))
    val localLookupMap = localLookup.toMap
    val localResult = localSource
      .flatMap {
        case (key, value) => localLookupMap.get(key).map(lv => key -> (value, lv))
      }
      .sorted

    def checkGood(rdd: RDD[(Int, (Long, Double))]) {
      assert(rdd.collect.toSeq.sorted == localResult)
    }

    import Implicits._
    val partitioner = new HashPartitioner(10)
    val sourceRDD = sparkContext.parallelize(localSource, 10)
    val lookupRDD = sparkContext.parallelize(localLookup).sortUnique(partitioner)

    implicit val rc = RuntimeContext(sparkContext, null, null, null, null)
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = true, 2000).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = true, 200).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = true, 20).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = true, 2).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = true, 0).lookup(lookupRDD))

    checkGood(HybridRDD.of(sourceRDD, partitioner, even = false, 2000).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = false, 200).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = false, 20).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = false, 2).lookup(lookupRDD))
    checkGood(HybridRDD.of(sourceRDD, partitioner, even = false, 0).lookup(lookupRDD))
  }

  test("lookup on empty RDD") {
    import Implicits._
    val partitioner = new HashPartitioner(1)
    val sourceRDD = sparkContext.emptyRDD[(Int, Long)]
    val lookupRDD = sparkContext.emptyRDD[(Int, Double)].sortUnique(partitioner)
    implicit val rc = RuntimeContext(sparkContext, null, null, null, null)

    assert(HybridRDD.of(sourceRDD, partitioner, even = true, 2000).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = true, 200).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = true, 20).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = true, 2).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = true, 0).lookup(lookupRDD).collect.isEmpty)

    assert(HybridRDD.of(sourceRDD, partitioner, even = false, 2000).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = false, 200).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = false, 20).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = false, 2).lookup(lookupRDD).collect.isEmpty)
    assert(HybridRDD.of(sourceRDD, partitioner, even = false, 0).lookup(lookupRDD).collect.isEmpty)
  }
}
