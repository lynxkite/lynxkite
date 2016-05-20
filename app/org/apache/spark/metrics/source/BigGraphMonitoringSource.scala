package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry
import com.lynxanalytics.biggraph.spark_util.BigGraphMetricSet

class BigGraphMonitoringSource extends Source {
  override val sourceName = "biggraph"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.registerAll(new BigGraphMetricSet)
}
