package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.{ DataManager, ThreadUtil }
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.ControlledFutures

class ExternalComputationTest extends OperationsTestBase {

  test("vertices table") {
    val t = box("Create example graph").box("SQL1").box("External computation", Map("label" -> "nothing")).table
    val e = intercept[Exception] {
      t.df.collect
    }
    assert(e.getCause.getMessage == "External computation 'nothing' must be executed externally.")
  }
}
