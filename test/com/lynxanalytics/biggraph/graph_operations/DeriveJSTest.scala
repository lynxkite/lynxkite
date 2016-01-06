package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveJSTest extends FunSuite with TestGraphOp {
  test("example graph: 'name.length * 10 + age'") {
    val expr = "name.length * 10 + age"
    val g = ExampleGraph()().result
    val op = DeriveJSDouble(
      JavaScript(expr),
      Seq("age", "name"))
    val derived = op(
      op.attrs,
      VertexAttributeToJSValue.seq(g.age.entity, g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 60.3, 1 -> 48.2, 2 -> 80.3, 3 -> 122.0))
  }

  ignore("example graph: global") {
    val expr = "global$greeting"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr), Seq() /* ,
      Seq("global$greeting") */ )
    // TODO
  }

  test("example graph: cons string gets converted back to String correctly") {
    val expr = "var res = 'a'; res += 'b'; res;"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr),
      Seq())
    val derived = op(op.vs, g.vertices)(op.attrs, Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> "ab", 1 -> "ab", 2 -> "ab", 3 -> "ab"))
  }

  test("example graph: \"gender == 'Male' ? 'Mr ' + name : 'Ms ' + name\"") {
    val expr = "gender == 'Male' ? 'Mr ' + name : 'Ms ' + name"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr),
      Seq("gender", "name"))
    val derived = op(
      op.attrs,
      VertexAttributeToJSValue.seq(g.gender.entity, g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(
      0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob", 3 -> "Mr Isolated Joe"))
  }

  test("DeriveJS works with no input attributes (vertices)") {
    val expr = "1.0"
    val g = ExampleGraph()().result
    val op = DeriveJSDouble(
      JavaScript(expr),
      Seq())
    val derived = op(op.vs, g.vertices.entity)(
      op.attrs,
      Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 3 -> 1.0))
  }

  test("JS integers become Scala doubles") {
    val g = ExampleGraph()().result

    val derived = DeriveJS.deriveFromAttributes[Double]("2", Seq(), g.vertices).attr
    assert(derived.rdd.collect.toSet == Set(0 -> 2.0, 1 -> 2.0, 2 -> 2.0, 3 -> 2.0))
  }

  test("DeriveJS works with no input attributes (edges)") {
    val expr = "'hallo'"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr),
      Seq())
    val derived = op(op.vs, g.edges.idSet)(
      op.attrs,
      Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> "hallo", 1 -> "hallo", 2 -> "hallo", 3 -> "hallo"))
  }

  test("Random weird types are not supported as input") {
    val g = ExampleGraph()().result
    intercept[AssertionError] {
      DeriveJS.deriveFromAttributes[Double](
        "location ? 1.0 : 2.0", Seq("location" -> g.location), g.vertices).attr
    }
  }

  test("We cannot simply access java stuff from JS") {
    val js = JavaScript("java.lang.System.out.println(3)")
    intercept[org.mozilla.javascript.EcmaError] {
      js.evaluate(Map(), classOf[Object])
    }
  }

  test("Utility methods") {
    val g = ExampleGraph()().result
    val nameHash = DeriveJS.deriveFromAttributes[Double](
      "util.hash(name)", Seq("name" -> g.name), g.vertices).attr
    assert(nameHash.rdd.collect.toSeq.sorted ==
      Seq(0 -> "Adam".hashCode.toDouble, 1 -> "Eve".hashCode.toDouble,
        2 -> "Bob".hashCode.toDouble, 3 -> "Isolated Joe".hashCode.toDouble))

    val rndSum = DeriveJS.deriveFromAttributes[Double](
      "var rnd = util.rnd(income); rnd.nextDouble() + rnd.nextDouble();",
      Seq("income" -> g.income),
      g.vertices).attr
    def rndSumScala(income: Double) = {
      val rnd = new scala.util.Random(income.toLong)
      rnd.nextDouble + rnd.nextDouble
    }
    assert(rndSum.rdd.collect.toSeq.sorted ==
      Seq(0 -> rndSumScala(1000.0), 2 -> rndSumScala(2000.0)))
  }
}
