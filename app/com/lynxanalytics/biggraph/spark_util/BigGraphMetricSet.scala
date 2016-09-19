package com.lynxanalytics.biggraph.spark_util

import com.codahale.metrics.Gauge
import com.codahale.metrics.MetricSet
import com.codahale.metrics.Metric

import org.apache.spark.metrics.source.BigGraphJvmMonitoringSource

import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory

object SetupMetricsSingleton {
  // Calling this method will instantiate this singleton.
  // The below code will only be run once per singleton.
  def dummy = ()

  val ms = org.apache.spark.SparkEnv.get.metricsSystem
  ms.registerSource(new BigGraphJvmMonitoringSource)
}

class BigGraphMetricSet extends MetricSet {

  val bufferPools: Iterable[BufferPoolMXBean] = {
    import scala.collection.JavaConverters._
    ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala
  }

  override def getMetrics(): java.util.Map[String, Metric] = {
    val gauges = new java.util.HashMap[String, Metric]()
    for (bean <- bufferPools) {
      val name = "buffer_pool." + bean.getName + "."
      gauges.put(
        name + "count",
        new Gauge[Long] {
          override def getValue: Long = bean.getCount
        })
      gauges.put(
        name + "used_bytes",
        new Gauge[Long] {
          override def getValue: Long = bean.getMemoryUsed
        })
      gauges.put(
        name + "capacity_bytes",
        new Gauge[Long] {
          override def getValue: Long = bean.getTotalCapacity
        })
    }
    return java.util.Collections.unmodifiableMap(gauges)
  }

}
