package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveScalaTest extends FunSuite with TestGraphOp {

  test("example graph: 'name.length * 10 + age'") {
    val expr = "name.length * 10 + age"
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq("age", "name"))
    val derived = op(
      op.attrs,
      Seq(g.age.entity, g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 60.3, 1 -> 48.2, 2 -> 80.3, 3 -> 122.0))
  }

  test("Spread out scalar to vertices") {
    val expr = "greeting"
    val g = ExampleGraph()().result
    val op = DeriveScala[String](
      expr,
      Seq(), Seq("greeting"))
    val derived = op(
      op.vs, g.vertices)(
        op.scalars, Seq(g.greeting.entity)).result.attr
    val elements = derived.rdd.collect()
    assert(elements.size == 4 && elements.forall(_._2 == "Hello world! 😀 "))
  }

  test("example graph: 'name.length * 10 + age + greeting.length'") {
    val expr = "name.length * 10 + age + greeting.length"
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq("age", "name"), Seq("greeting"))
    val derived = op(
      op.attrs,
      Seq(g.age.entity, g.name.entity))(
        op.scalars, Seq(g.greeting.entity)).result.attr
    assert(derived.rdd.collect.sorted.toList == List(0 -> 76.3, 1 -> 64.2, 2 -> 96.3, 3 -> 138.0))
  }

  test("example graph: cons string gets converted back to String correctly") {
    val expr = """
      var res = "a"
      res += "b"
      res"""
    val g = ExampleGraph()().result
    val op = DeriveScala[String](
      expr,
      Seq())
    val derived = op(op.vs, g.vertices)(op.attrs, Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> "ab", 1 -> "ab", 2 -> "ab", 3 -> "ab"))
  }

  test("example graph: \"if (gender == \"Male\") \"Mr \" + name else \"Ms \" + name\"") {
    val expr = "if (gender == \"Male\") \"Mr \" + name else \"Ms \" + name"
    val g = ExampleGraph()().result
    val op = DeriveScala[String](
      expr,
      Seq("gender", "name"))
    val derived = op(
      op.attrs,
      Seq(g.gender.entity, g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(
      0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob", 3 -> "Mr Isolated Joe"))
  }

  test("DeriveScala works with no input attributes (vertices)") {
    val expr = "1.0"
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq())
    val derived = op(op.vs, g.vertices.entity)(
      op.attrs,
      Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 3 -> 1.0))
  }

  test("Many input attributes to check correct param substitution") {
    val expr = "age.toString + income.toString + name + gender"
    val g = ExampleGraph()().result
    val op = DeriveScala[String](
      expr,
      Seq("age", "income", "name", "gender"))
    val derived = op(op.vs, g.vertices.entity)(
      op.attrs,
      Seq(g.age.entity, g.income.entity, g.name.entity, g.gender.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> "20.31000.0AdamMale", 2 -> "50.32000.0BobMale"))
  }

  test("DeriveScala works with no input attributes (edges)") {
    val expr = "\"hallo\""
    val g = ExampleGraph()().result
    val op = DeriveScala[String](
      expr,
      Seq())
    val derived = op(op.vs, g.edges.idSet)(
      op.attrs,
      Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> "hallo", 1 -> "hallo", 2 -> "hallo", 3 -> "hallo"))
  }

  test("Utility methods") {
    val g = ExampleGraph()().result
    val nameHash = DeriveScala.deriveFromAttributes(
      "name.hashCode.toDouble", Seq("name" -> g.name), g.vertices).runtimeSafeCast[Double]
    assert(nameHash.rdd.collect.toSeq.sorted ==
      Seq(0 -> "Adam".hashCode.toDouble, 1 -> "Eve".hashCode.toDouble,
        2 -> "Bob".hashCode.toDouble, 3 -> "Isolated Joe".hashCode.toDouble))

    val rndSum = DeriveScala.deriveFromAttributes("""
      var rnd = new scala.util.Random(income.toLong)
      rnd.nextDouble() + rnd.nextDouble()""",
      Seq("income" -> g.income),
      g.vertices).runtimeSafeCast[Double]
    def rndSumScala(income: Double) = {
      val rnd = new scala.util.Random(income.toLong)
      rnd.nextDouble + rnd.nextDouble
    }
    assert(rndSum.rdd.collect.toSeq.sorted ==
      Seq(0 -> rndSumScala(1000.0), 2 -> rndSumScala(2000.0)))
  }

  test("example graph - all vertices: income == null") {
    val expr = "income.map(_ * 10.0).getOrElse(666.0)"
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq("income"),
      onlyOnDefinedAttrs = false)
    val derived = op(
      op.attrs,
      Seq(g.income.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set((0, 10000.0), (1, 666.0), (2, 20000.0), (3, 666.0)))
  }

  test("example graph - all vertices: Option to Option") {
    val expr = "income"
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq("income"),
      onlyOnDefinedAttrs = false)
    val derived = op(
      op.attrs,
      Seq(g.income.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set((0, 1000.0), (2, 2000.0)))
  }

  test("example graph - all vertices: Option[Double] * 10 throws error") {
    val expr = "income * 10.0"
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq("income"),
      onlyOnDefinedAttrs = false)
    intercept[javax.script.ScriptException] { // Script throws a compilation error.
      val derived = op(op.attrs, Seq(g.income.entity)).result.attr
      derived.rdd.collect
    }
  }

  test("example graph - all vertices: two attributes") {
    val expr = "income.map(_ + age.get)"
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq("income", "age"),
      onlyOnDefinedAttrs = false)
    val derived = op(
      op.attrs,
      Seq(g.income.entity, g.age.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set((0, 1020.3), (2, 2050.3)))
  }

  test("example graph - all vertices: two attributes wrong type") {
    val expr = "income + age" // Fails because income and age are Option[Double]-s.
    val g = ExampleGraph()().result
    val op = DeriveScala[Double](
      expr,
      Seq("income", "age"),
      onlyOnDefinedAttrs = false)
    val derived = op(
      op.attrs,
      Seq(g.income.entity, g.age.entity)).result.attr
    intercept[javax.script.ScriptException] {
      derived.rdd.collect
    }
  }

  def checkScala(expr: String, name: String, attr: Attribute[_], result: Set[(Int, String)]) = {
    val op = DeriveScala[String](expr, Seq(name))
    val derived = op(op.attrs, Seq(attr)).result.attr
    assert(derived.rdd.collect.toSet == result)
  }

  test("example graph - arrays") {
    val g = ExampleGraph()().result
    val ages = {
      val op = AggregateByEdgeBundle(Aggregator.AsVector[Double]())
      op(op.connection, g.edges)(op.attr, g.age).result.attr
    }
    val genders = {
      val op = AggregateByEdgeBundle(Aggregator.AsVector[String]())
      op(op.connection, g.edges)(op.attr, g.gender).result.attr
    }
    checkScala("ages.toString()", "ages", ages,
      Set(0 -> "Vector(18.2, 50.3)", 1 -> "Vector(20.3, 50.3)"))
    checkScala("genders.toString()", "genders", genders,
      Set(0 -> "Vector(Female, Male)", 1 -> "Vector(Male, Male)"))
    checkScala("ages.length.toString()", "ages", ages, Set(0 -> "2", 1 -> "2"))
    checkScala("genders.length.toString()", "genders", genders, Set(0 -> "2", 1 -> "2"))
    checkScala("ages(0).toString()", "ages", ages, Set(0 -> "18.2", 1 -> "20.3"))
    checkScala("genders(0)", "genders", genders, Set(0 -> "Female", 1 -> "Male"))
    checkScala("(ages :+ 100.0).toString()", "ages", ages,
      Set(0 -> "Vector(18.2, 50.3, 100.0)", 1 -> "Vector(20.3, 50.3, 100.0)"))
    checkScala("(genders :+ \"abc\").toString()", "genders", genders,
      Set(0 -> "Vector(Female, Male, abc)", 1 -> "Vector(Male, Male, abc)"))
  }

  test("example graph - arrays of arrays") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1)))().result
    val age = AddVertexAttribute.run(g.vs, Map(0 -> "10", 1 -> "20", 2 -> "30"))
    val ages = {
      val op = AggregateByEdgeBundle(Aggregator.AsVector[String]())
      op(op.connection, g.es)(op.attr, age).result.attr
    }
    val agess = {
      val op = AggregateByEdgeBundle(Aggregator.AsVector[Vector[String]]())
      op(op.connection, g.es)(op.attr, ages).result.attr
    }
    checkScala("agess.length.toString()", "agess", agess, Set(0 -> "2", 1 -> "2", 2 -> "2"))
    checkScala("agess.toString()", "agess", agess, Set(
      0 -> "Vector(Vector(10, 30), Vector(10, 20))",
      1 -> "Vector(Vector(20, 30), Vector(10, 20))",
      2 -> "Vector(Vector(20, 30), Vector(10, 30))"))
    checkScala("agess(0).toString()", "agess", agess,
      Set(0 -> "Vector(10, 30)", 1 -> "Vector(20, 30)", 2 -> "Vector(20, 30)"))
    checkScala("agess(0)(0).toString()", "agess", agess, Set(0 -> "10", 1 -> "20", 2 -> "20"))
  }
}
