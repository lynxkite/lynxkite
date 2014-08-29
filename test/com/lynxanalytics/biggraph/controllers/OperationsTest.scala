package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class OperationsTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val controller = new BigGraphController(this)
  val ops = new Operations(controller)
  val project = Project("Test_Project")
  project.notes = "test project" // Make sure project directory exists.

  def run(op: String, params: Map[String, String] = Map(), on: Project = project) =
    ops.apply(
      ProjectOperationRequest(on.projectName, FEOperationSpec(op.replace(" ", "-"), params)))

  test("Derived vertex attribute (Double)") {
    run("Example Graph")
    run("Derived vertex attribute",
      Map("output" -> "output", "expr" -> "100 + age + 10 * name.length"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 160.3, 1 -> 148.2, 2 -> 180.3, 3 -> 222.0))
  }

  test("Derived vertex attribute (String)") {
    run("Example Graph")
    run("Derived vertex attribute",
      Map("output" -> "output", "expr" -> "gender == 'Male' ? 'Mr ' + name : 'Ms ' + name"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[String]
    assert(attr.rdd.collect.toMap == Map(0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob", 3 -> "Mr Isolated Joe"))
  }

  test("Aggregate to segmentation") {
    run("Example Graph")
    run("Connected components", Map("name" -> "cc"))
    run("Aggregate to segmentation",
      Map("segmentation" -> "cc", "aggregate-age" -> "average", "aggregate-name" -> "count", "aggregate-gender" -> "majority_100"))
    val seg = project.segmentation("cc").project
    val age = seg.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(19.25, 50.3, 2.0))
    val count = seg.vertexAttributes("name_count").runtimeSafeCast[Double]
    assert(count.rdd.collect.toMap.values.toSet == Set(2.0, 1.0, 1.0))
    val gender = seg.vertexAttributes("gender_majority_100").runtimeSafeCast[String]
    assert(gender.rdd.collect.toMap.values.toSeq.sorted == Seq("", "Male", "Male"))
  }

  test("Join vertices on attribute") {
    run("Example Graph")
    run("Join vertices on attribute",
      Map("attr" -> "gender", "aggregate-age" -> "average", "aggregate-name" -> "count"))
    val age = project.vertexAttributes("age").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(24.2, 18.2))
    val count = project.vertexAttributes("name").runtimeSafeCast[Double]
    assert(count.rdd.collect.toMap.values.toSet == Set(3.0, 1.0))
  }

  test("Aggregate edge attribute") {
    run("Example Graph")
    run("Aggregate edge attribute", Map("prefix" -> "", "aggregate-weight" -> "sum"))
    assert(project.scalars("weight_sum").value == 10.0)
  }

  test("Restore checkpoint after failing operation") {
    class Bug extends Exception("simulated bug")
    ops.register(new Operation(_, "Test operations") {
      val title = "Buggy op"
      def enabled = ???
      def parameters = ???
      def apply(params: Map[String, String]) = {
        project.vertexSet = null
        throw new Bug
      }
    })
    run("Example Graph")
    assert(project.vertexSet != null)
    try {
      run("Buggy op")
    } catch {
      case _: Bug =>
    }
    assert(project.vertexSet != null)
  }

  test("Project union") {
    run("Example Graph")
    val other = Project("ExampleGraph2")
    project.copy(other)
    run("Rename vertex attribute", Map("from" -> "age", "to" -> "newage"), on = other)
    run("Rename edge attribute", Map("from" -> "comment", "to" -> "newcomment"), on = other)
    run("Union with another project", Map("other" -> "ExampleGraph2"))
  }
}
