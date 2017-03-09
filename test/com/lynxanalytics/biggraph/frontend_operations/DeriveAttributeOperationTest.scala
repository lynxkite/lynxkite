package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveAttributeOperationTest extends OperationsTestBase {
  test("Derive vertex attribute (Double)") {
    run("Create example graph")
    run("Derive vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> "100 + age + 10 * name.length"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 160.3, 1 -> 148.2, 2 -> 180.3, 3 -> 222.0))
  }

  test("Multi-line function") {
    run("Create example graph")
    run("Derive vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> """
        (function() {
          return age;
        })()"""))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
  }

  test("Multi-line expression and utility function") {
    run("Create example graph")
    run("Derive vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> """
        var rnd = util.rnd(income);
        rnd.nextDouble() + rnd.nextDouble();"""))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    def rndSumScala(income: Double) = {
      val rnd = new scala.util.Random(income.toLong)
      rnd.nextDouble + rnd.nextDouble
    }
    assert(attr.rdd.collect.toMap == Map(0 -> rndSumScala(1000.0), 2 -> rndSumScala(2000.0)))
  }

  test("Vector attribute") {
    run("Create example graph")
    run("Aggregate on neighbors",
      Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate-name" -> "vector"))
    run("Derive vertex attribute",
      Map("type" -> "string", "output" -> "output", "expr" -> """
        (function() { neighbor_name_vector.sort(); return neighbor_name_vector[0]; })()"""))
    val attr = project.vertexAttributes("output").runtimeSafeCast[String]
    assert(attr.rdd.collect.toMap == Map(0 -> "Bob", 1 -> "Adam", 2 -> "Adam"))
  }

  test("Primitive vector attribute") {
    run("Create example graph")
    run("Aggregate on neighbors",
      Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate-age" -> "vector"))
    run("Derive vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> """
        (function() {
           if (neighbor_age_vector.length > 0) {
             neighbor_age_vector.sort();
             return neighbor_age_vector[0] * 1;
           }
           return undefined;
         })()"""))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 18.2, 1 -> 20.3, 2 -> 18.2))
  }

  test("Vector of vector attribute") {
    run("Create example graph")
    run("Aggregate on neighbors",
      Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate-age" -> "vector"))
    run("Aggregate on neighbors",
      Map(
        "prefix" -> "neighbor",
        "direction" -> "all edges",
        "aggregate-neighbor_age_vector" -> "vector"))
    run("Derive vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> """
        neighbor_neighbor_age_vector_vector.map(function(subarray) {
          return subarray.reduce(function(a, b) { return a + b; }, 0);
        }).reduce(function(a, b) { return a + b; }, 0)"""))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 220.3, 1 -> 211.89999999999998, 2 -> 177.6))
  }

  test("Vector length") {
    run("Create example graph")
    run("Aggregate on neighbors",
      Map("prefix" -> "neighbor", "direction" -> "all edges", "aggregate-name" -> "vector"))
    run("Derive vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> "neighbor_name_vector.length"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 3, 1 -> 3, 2 -> 2))
  }

  test("Wrong type") {
    val e = intercept[org.apache.spark.SparkException] {
      run("Derive vertex attribute",
        Map("type" -> "double", "output" -> "output", "expr" -> "'hello'"))
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
    run("Create example graph")
    run("Rename vertex attribute", Map("from" -> "income", "to" -> "nam"))
    run("Derive vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> "100 + age + 10 * name.length"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.size == 4)
  }

  test("Derive vertex attribute (String)") {
    run("Create example graph")
    // Test dropping values.
    run("Derive vertex attribute",
      Map("type" -> "string", "output" -> "gender",
        "expr" -> "name == 'Isolated Joe' ? undefined : gender"))
    run("Derive vertex attribute",
      Map("type" -> "string", "output" -> "output",
        "expr" -> "gender == 'Male' ? 'Mr ' + name : 'Ms ' + name"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[String]
    assert(attr.rdd.collect.toMap == Map(0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob"))
  }

  // TODO: Re-enable this test. See #1037.
  ignore("Derive edge attribute") {
    run("Create example graph")
    // Test dropping values.
    run("Derive edge attribute",
      Map("type" -> "string", "output" -> "tripletke",
        "expr" -> "src$name + ':' + comment + ':' + dst$age + '#' + weight"))
    val attr = project.edgeAttributes("tripletke").runtimeSafeCast[String]
    assert(attr.rdd.collect.toSeq == Seq(
      (0, "Adam:Adam loves Eve:18.2#1"),
      (1, "Eve:Eve loves Adam:20.3#2"),
      (2, "Bob:Bob envies Adam:20.3#3"),
      (3, "Bob:Bob loves Eve:18.2#4")))
  }

  test("Derive vertex attribute (Vector of Strings)") {
    run("Create example graph")
    run("Derive vertex attribute",
      Map("type" -> "vector of strings", "output" -> "vector", "expr" -> "[gender]"))
    val attr = project.vertexAttributes("vector").runtimeSafeCast[Vector[String]]
    assert(attr.rdd.collect.toMap == Map(
      0 -> Vector("Male"), 1 -> Vector("Female"), 2 -> Vector("Male"), 3 -> Vector("Male")))
  }

  test("Derive vertex attribute (Vector of Doubles)") {
    run("Create example graph")
    run("Derive vertex attribute",
      Map("type" -> "vector of doubles", "output" -> "vector", "expr" -> "[age]"))
    val attr = project.vertexAttributes("vector").runtimeSafeCast[Vector[Double]]
    assert(attr.rdd.collect.toMap == Map(
      0 -> Vector(20.3), 1 -> Vector(18.2), 2 -> Vector(50.3), 3 -> Vector(2.0)))
  }

  test("Derive vertex attribute (does not return vector)") {
    run("Create example graph")
    val e = intercept[org.apache.spark.SparkException] {
      run("Derive vertex attribute",
        Map("type" -> "vector of strings", "output" -> "vector", "expr" -> "gender"))
      project.vertexAttributes("vector").runtimeSafeCast[Vector[String]].rdd.collect
    }
    assert(e.getCause.getMessage == "assertion failed: JavaScript(gender) with values: " +
      "{gender: Male} did not return a vector: Male")
  }

  test("Derive vertex attribute (wrong vector generic type)") {
    run("Create example graph")
    val e = intercept[org.apache.spark.SparkException] {
      run("Derive vertex attribute",
        Map("type" -> "vector of doubles", "output" -> "vector", "expr" -> "[gender]"))
      project.vertexAttributes("vector").runtimeSafeCast[Vector[Double]].rdd.collect
    }
    assert(e.getCause.getMessage == "assertion failed: JavaScript([gender]) with values: " +
      "{gender: Male} did not return a number in vector: NaN")
  }

  test("Derive vertex attribute (undefined in vector)") {
    run("Create example graph")
    val e = intercept[org.apache.spark.SparkException] {
      run("Derive vertex attribute", Map("type" -> "vector of doubles",
        "output" -> "vector", "defined_attrs" -> "false", "expr" -> "[income]"))
      project.vertexAttributes("vector").runtimeSafeCast[Vector[Double]].rdd.collect
    }
    assert(e.getCause.getMessage == "assertion failed: JavaScript([income]) with values: " +
      "{income: undefined} returned undefined element in vector: null")
  }
}
