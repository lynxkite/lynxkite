package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry
import com.lynxanalytics.biggraph.spark_util.BigGraphMetricSet

class BigGraphJvmMonitoringSource extends Source {
  override val sourceName = "biggraph_jvm"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.registerAll(new BigGraphMetricSet)
}
