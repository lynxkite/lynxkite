/*package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class ImportGraphTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("In-development test for graph importing") {
    val graphManager = cleanGraphManager("importgraph")
    val dataManager = cleanDataManager("importgraph")
    val origGraph = graphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val data = dataManager.obtainData(origGraph)
  }
}
*/