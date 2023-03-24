package com.lynxanalytics.biggraph.partitioned_parquet

import com.lynxanalytics.biggraph.TestSparkContext

class PartitionedParquetTest extends org.scalatest.funsuite.AnyFunSuite with TestSparkContext {
  test("read") {
    val ss = sparkSession
    val df = ss.createDataFrame(Seq((1, 2), (3, 4))).repartition(12)
    val origPartitions = df.rdd.map(_.toSeq.toList).glom.collect.toList.map(_.toList)
    assert(origPartitions ==
      List(Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil, List(List(3, 4)), List(List(1, 2)), Nil))
    df.write.mode("overwrite").parquet("test.parquet")
    val back = ss.read
      .option("partitions", "12")
      .format(PartitionedParquet.format)
      .load("test.parquet")
    val backPartitions = df.rdd.map(_.toSeq.toList).glom.collect.toList.map(_.toList)
    assert(backPartitions == origPartitions)
  }
}
