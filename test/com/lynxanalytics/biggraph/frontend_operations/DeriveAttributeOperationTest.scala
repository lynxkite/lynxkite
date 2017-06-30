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

  test("The containsIdentifierJS function identifier name ending characters") {
    val expr =
      """a+b-c*d/e%f==g.h,i;j:k'l"m`n
         !o@p#q(r{s[t]u}v)w^x>y<z"""

    val identified = ('a' to 'z').map(i => i -> JSUtilities.containsIdentifierJS(expr, i.toString)).toMap
    val shouldBe = ('a' to 'z').map(i => i -> true).toMap
    assert(identified == shouldBe)
  }

  test("The containsIdentifierJS function with substring conflicts") {
    val expr = "ArsenalFC and FCBarcelona are$the \\be\\sts."
    val testResults = Map(
      "Match starting substring" -> JSUtilities.containsIdentifierJS(expr, "Arsenal"),
      "Match ending substring" -> JSUtilities.containsIdentifierJS(expr, "Barcelona"),
      "Match with $ on right side" -> JSUtilities.containsIdentifierJS(expr, "are"),
      "Match with $ on left side" -> JSUtilities.containsIdentifierJS(expr, "the"),
      "Finds identifiers with special regex characters" -> JSUtilities.containsIdentifierJS(expr, "\\be\\sts")
    )
    val resultShouldBe = Map(
      "Match starting substring" -> false,
      "Match ending substring" -> false,
      "Match with $ on right side" -> false,
      "Match with $ on left side" -> false,
      "Finds identifiers with special regex characters" -> true
    )
    assert(testResults == resultShouldBe)
  }

  // See #5567
  test("The containsIdentifierJS function with attr names that are valid JS literals") {
    assert(false == JSUtilities.containsIdentifierJS("1 + 1", "1"))
    assert(false == JSUtilities.containsIdentifierJS("'a' + a", "'a'"))
    assert(false == JSUtilities.containsIdentifierJS("a = 1", "b"))
    assert(true == JSUtilities.containsIdentifierJS("a = 1", "a"))
    assert(true == JSUtilities.containsIdentifierJS("$a = 1", "$a"))
    assert(true == JSUtilities.containsIdentifierJS("_a = 1", "_a"))
    assert(true == JSUtilities.containsIdentifierJS("\\u0061 = 1", "\\u0061"))
    // The following should be true according to ES5 spec, but we don't get it.
    // assert(true == JSUtilities.containsIdentifierJS("\\u0061 = 1", "a"))
    assert(false == JSUtilities.containsIdentifierJS("a + b + c", "a + b"))
  }

  test("Derive vertex attribute with substring conflict (#1676)") {
    val project = box("Create example graph")
      .box("Rename vertex attribute", Map("before" -> "income", "after" -> "nam"))
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

  // TODO: Re-enable this test. See #1037.
  ignore("Derive edge attribute") {
    val project = box("Create example graph")
      // Test dropping values.
      .box("Derive edge attribute",
        Map("output" -> "tripletke",
          "expr" -> "src$name + ':' + comment + ':' + dst$age + '#' + weight"))
      .project
    val attr = project.edgeAttributes("tripletke").runtimeSafeCast[String]
    assert(attr.rdd.collect.toSeq == Seq(
      (0, "Adam:Adam loves Eve:18.2#1"),
      (1, "Eve:Eve loves Adam:20.3#2"),
      (2, "Bob:Bob envies Adam:20.3#3"),
      (3, "Bob:Bob loves Eve:18.2#4")))
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
