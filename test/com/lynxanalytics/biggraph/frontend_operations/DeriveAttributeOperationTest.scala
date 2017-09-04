package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveAttributeOperationTest extends OperationsTestBase {

  test("Derive vertex attribute (Double)") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> "100 + age + 10 * name.length"))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 160.3, 1 -> 148.2, 2 -> 180.3, 3 -> 222.0))
  }

  test("Derive vertex attribute - back quote") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> "`age`"))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
  }

  test("Derive vertex attribute - back quote crazy identifier") {
    val project = box("Create example graph")
      .box("Add constant vertex attribute",
        Map("name" -> "123 weird # name", "type" -> "Double", "value" -> "0.0"))
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> "`123 weird # name`"))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 0.0))
  }

  test("Multi-line function") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> """
        def a() = {
          age
        }
        a()"""))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
  }

  test("Multi-line expression and utility function") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> """
        var rnd = new scala.util.Random(income.toLong)
        rnd.nextDouble() + rnd.nextDouble()"""))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    def rndSumScala(income: Double) = {
      val rnd = new scala.util.Random(income.toLong)
      rnd.nextDouble + rnd.nextDouble
    }
    assert(attr.rdd.collect.toMap == Map(0 -> rndSumScala(1000.0), 2 -> rndSumScala(2000.0)))
  }

  test("Vector attribute") {
    val project = box("Create example graph")
      .box("Aggregate on neighbors",
        Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate_name" -> "vector"))
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> """
          val sorted = neighbor_name_vector.sorted
          sorted(0)"""))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[String]
    assert(attr.rdd.collect.toMap == Map(0 -> "Bob", 1 -> "Adam", 2 -> "Adam"))
  }

  test("Primitive vector attribute") {
    val project = box("Create example graph")
      .box("Aggregate on neighbors",
        Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate_age" -> "vector"))
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> """
        def f() = {
           if (neighbor_age_vector.length > 0) {
             val sorted = neighbor_age_vector.sorted
             Some(sorted(0) * 1)
           } else {
             None
           }
         }
       f()"""))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 18.2, 1 -> 20.3, 2 -> 18.2))
  }

  test("Vector of vector attribute") {
    val project = box("Create example graph")
      .box("Aggregate on neighbors",
        Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate_age" -> "vector"))
      .box("Aggregate on neighbors",
        Map(
          "prefix" -> "neighbor",
          "direction" -> "all edges",
          "aggregate_neighbor_age_vector" -> "vector"))
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> """
        neighbor_neighbor_age_vector_vector.map({ subarray =>
          subarray.reduce(_ + _)
        }).reduce(_ + _)"""))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 220.3, 1 -> 211.89999999999998, 2 -> 177.6))
  }

  test("Vector length") {
    val project = box("Create example graph")
      .box("Aggregate on neighbors",
        Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate_name" -> "vector"))
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> "neighbor_name_vector.length.toDouble"))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 3.0, 1 -> 3.0, 2 -> 2.0))
  }

  ignore("Wrong type") {
    val e = intercept[org.apache.spark.SparkException] {
      val project = box("Create example graph")
        .box("Derive vertex attribute",
          Map("output" -> "output", "expr" -> "'hello'"))
        .project
      project.vertexAttributes("output").runtimeSafeCast[Double].rdd.collect
    }
    assert(e.getCause.getMessage ==
      "assertion failed: JavaScript('hello') with values: {} did not return a number: NaN")
  }

  test("ScalaUtilities finds identifiers") {
    assert(true == ScalaUtilities.containsIdentifier("`age`", "age"))
    assert(true == ScalaUtilities.containsIdentifier("`123 weird id #?!`", "123 weird id #?!"))
    assert(true == ScalaUtilities.containsIdentifier("age", "age"))
    assert(true == ScalaUtilities.containsIdentifier(" age ", "age"))
    assert(true == ScalaUtilities.containsIdentifier("src$age", "src$age"))
    assert(true == ScalaUtilities.containsIdentifier("age - name", "age"))
    assert(true == ScalaUtilities.containsIdentifier("age_v2", "age_v2"))
    assert(true == ScalaUtilities.containsIdentifier("age.toString", "age"))
    assert(true == ScalaUtilities.containsIdentifier("age\n1.0", "age"))

    assert(false == ScalaUtilities.containsIdentifier("name", "nam"))
    assert(false == ScalaUtilities.containsIdentifier("name", "ame"))
    assert(false == ScalaUtilities.containsIdentifier("nam", "name"))
    assert(false == ScalaUtilities.containsIdentifier("ame", "name"))
    assert(false == ScalaUtilities.containsIdentifier("Name", "Name"))
    assert(false == ScalaUtilities.containsIdentifier("\"name\"", "name"))
    assert(false == ScalaUtilities.containsIdentifier("'name", "name"))
  }

  test("Derive vertex attribute with substring conflict (#1676)") {
    val project = box("Create example graph")
      .box("Rename vertex attributes", Map("change_income" -> "nam"))
      .box("Derive vertex attribute",
        Map("output" -> "output", "expr" -> "100 + age + 10 * name.length"))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.size == 4)
  }

  test("Derive vertex attribute (String)") {
    val project = box("Create example graph")
      // Test dropping values.
      .box("Derive vertex attribute",
        Map("output" -> "gender",
          "expr" -> "if (name == \"Isolated Joe\") None else Some(gender)"))
      .box("Derive vertex attribute",
        Map("output" -> "output",
          "expr" -> "if (gender == \"Male\") \"Mr \" + name else \"Ms \" + name"))
      .project
    val attr = project.vertexAttributes("output").runtimeSafeCast[String]
    assert(attr.rdd.collect.toMap == Map(0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob"))
  }

  test("Derive edge attribute") {
    val project = box("Create example graph")
      // Test dropping values.
      .box("Derive edge attribute",
        Map("output" -> "tripletke",
          "expr" -> "src$name + ':' + comment + ':' + dst$age + '#' + weight"))
      .project
    val attr = project.edgeAttributes("tripletke").runtimeSafeCast[String]
    assert(attr.rdd.collect.toSeq == Seq(
      (0, "Adam:Adam loves Eve:18.2#1.0"),
      (1, "Eve:Eve loves Adam:20.3#2.0"),
      (2, "Bob:Bob envies Adam:20.3#3.0"),
      (3, "Bob:Bob loves Eve:18.2#4.0")))
  }

  test("Derive vertex attribute (Vector of Strings)") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "vector", "expr" -> "Vector(gender)"))
      .project
    val attr = project.vertexAttributes("vector").runtimeSafeCast[Vector[String]]
    assert(attr.rdd.collect.toMap == Map(
      0 -> Vector("Male"), 1 -> Vector("Female"), 2 -> Vector("Male"), 3 -> Vector("Male")))
  }

  test("Derive vertex attribute (Vector of Doubles)") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "vector", "expr" -> "Vector(age)"))
      .project
    val attr = project.vertexAttributes("vector").runtimeSafeCast[Vector[Double]]
    assert(attr.rdd.collect.toMap == Map(
      0 -> Vector(20.3), 1 -> Vector(18.2), 2 -> Vector(50.3), 3 -> Vector(2.0)))
  }

  ignore("Derive vertex attribute (does not return vector)") {
    val e = intercept[org.apache.spark.SparkException] {
      box("Create example graph")
        .box("Derive vertex attribute",
          Map("output" -> "vector", "expr" -> "gender"))
        .project.vertexAttributes("vector").runtimeSafeCast[Vector[String]].rdd.collect
    }
    assert(e.getCause.getMessage == "assertion failed: JavaScript(gender) with values: " +
      "{gender: Male} did not return a vector: Male")
  }

  ignore("Derive vertex attribute (wrong vector generic type)") {
    val e = intercept[org.apache.spark.SparkException] {
      box("Create example graph")
        .box("Derive vertex attribute",
          Map("type" -> "Vector of Doubles", "output" -> "vector", "expr" -> "Vector(gender)"))
        .project.vertexAttributes("vector").runtimeSafeCast[Vector[Double]].rdd.collect
    }
    assert(e.getCause.getMessage == "assertion failed: JavaScript([gender]) with values: " +
      "{gender: Male} did not return a number in vector: NaN")
  }
}
