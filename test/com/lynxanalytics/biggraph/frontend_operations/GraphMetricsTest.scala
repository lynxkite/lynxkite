package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import org.apache.spark

class GraphMetricsTest extends OperationsTestBase {
  test("graph metrics works") {
    val table = box("Create example graph")
      .box("built-ins/graph-metrics", Map(
        "CustomPercentile" -> "0",
        "MaximumNumberOfIterations" -> "30",
         "MinimalModularityIncerement" -> "0.001",
         "ModularClustering" -> "true"))
  }
}
