package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class SQLTest extends OperationsTestBase {
  test("SQL basics") {
    val table = box("Create example graph")
      .box("SQL", Map("sql" -> "select * from `stuff|vertices`"))
      .table
    assert(table.df.collect == null)
  }
}
