package com.lynxanalytics.biggraph.serving

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.controllers._
import org.scalatest.FunSuite

class RemoteAPITest extends FunSuite with TestGraphOp {
  val ctrl = new RemoteAPIController(this)

  test("no temp table name collision") {
    import RemoteAPIProtocol._
    val u = User.fake
    val eg = Box("eg", "Example Graph", Map(), 0, 0, Map())
    val ws = Workspace(List(eg))
    /*
    val example = ctrl.updateWorkspace(u, OperationRequest(empty, Map(), "examplegraph", Map())).checkpoint
    val view1 = ctrl.createView(
      u, GlobalSQLRequest("select name as n from x", Map("x" -> example))).checkpoint
    val view2 = ctrl.createView(
      u, GlobalSQLRequest("select n from x", Map("x" -> view1, "y" -> view1))).checkpoint
    val future = ctrl.takeFromView(u, TakeFromViewRequest(view2, 10))
    // The issue at the time of writing this test happens during the creation of the "view2"
    // DataFrame. First the DataFrame for "view1" is created. This is registered as "x". Then the
    // we run the same code again, to register the result as "y". But createDataFrame for "view1"
    // also registers a DataFrame as "x". When this code runs the second time, it overwrites the
    // registration where we put "view1" in "x". When we then evaluate "select n from x", "x" will
    // actually point to the vertex table of the example graph, which has no "n" column.
    val result = concurrent.Await.result(future, concurrent.duration.Duration.Inf).rows
    assert(result.length == 4)
    */
  }
}
