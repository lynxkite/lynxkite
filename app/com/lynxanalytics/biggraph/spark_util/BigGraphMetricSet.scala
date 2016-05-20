package com.lynxanalytics.biggraph.spark_util

import com.codahale.metrics.Gauge
import com.codahale.metrics.MetricSet
import com.codahale.metrics.Metric

import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory

class BigGraphMetricSet extends MetricSet {

  val bufferPools: Iterable[BufferPoolMXBean] = {
    import scala.collection.JavaConverters._
    ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala
  }

  override def getMetrics(): java.util.Map[String, Metric] = {
    val gauges = new java.util.HashMap[String, Metric]()
    for (bean <- bufferPools) {
      val name = "bufferpool." + bean.getName
      gauges.put(
        name + ".count",
        new Gauge[Long] {
          override def getValue: Long = bean.getCount
        })
      gauges.put(
        name + ".used",
        new Gauge[Long] {
          override def getValue: Long = bean.getMemoryUsed
        })
      gauges.put(
        name + ".capacity",
        new Gauge[Long] {
          override def getValue: Long = bean.getTotalCapacity
        })
    }
    return java.util.Collections.unmodifiableMap(gauges)
  }

}
