package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SphynxOnly
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ComputeInPythonTest extends OperationsTestBase {
  test("example graph", SphynxOnly) {
    val p = box("Create example graph")
      .box(
        "Compute in Python",
        Map(
          "inputs" -> "vs.name, vs.age, es.weight, es.comment, es.src, es.dst, graph_attributes.greeting",
          "outputs" -> "vs.with_title: str, vs.age_squared: float, es.score: float, es.names: str, graph_attributes.hello: str, graph_attributes.average_age: float",
          "code" -> """
vs['with_title'] = 'The Honorable ' + vs.name
vs['age_squared'] = vs.age ** 2
es['score'] = es.weight + es.comment.str.len()
es['names'] = 'from ' + vs.name[es.src].values + ' to ' + vs.name[es.dst].values
graph_attributes.hello = graph_attributes.greeting.lower()
graph_attributes.average_age = vs.age.mean()
          """,
        ),
      )
      .project
    assert(
      get(p.vertexAttributes("with_title").runtimeSafeCast[String]) ==
        Map(
          0 -> "The Honorable Adam",
          1 -> "The Honorable Eve",
          2 -> "The Honorable Bob",
          3 -> "The Honorable Isolated Joe"))
    assert(
      get(p.vertexAttributes("age_squared").runtimeSafeCast[Double]).mapValues(_.round) ==
        Map(0 -> 412, 1 -> 331, 2 -> 2530, 3 -> 4))
    assert(
      get(p.edgeAttributes("score").runtimeSafeCast[Double]) ==
        Map(0 -> 15.0, 1 -> 16.0, 2 -> 18.0, 3 -> 17.0))
    assert(
      get(p.edgeAttributes("names").runtimeSafeCast[String]) ==
        Map(0 -> "from Adam to Eve", 1 -> "from Eve to Adam", 2 -> "from Bob to Adam", 3 -> "from Bob to Eve"))
    assert(get(p.scalars("hello").runtimeSafeCast[String]) == "hello world! 😀 ")
    assert(get(p.scalars("average_age").runtimeSafeCast[Double]).round == 23)
  }

  test("vectors", SphynxOnly) {
    val p = box("Create example graph")
      .box(
        "Compute in Python",
        Map(
          "inputs" -> "vs.age",
          "outputs" -> "vs.v: np.ndarray",
          "code" -> """
v = np.array([[1, 2]]) * vs.age[:, None]
vs['v'] = v.tolist()
          """),
      )
      .box(
        "Compute in Python",
        Map(
          "inputs" -> "vs.v",
          "outputs" -> "vs.s: float",
          "code" -> """
vs['s'] = np.stack(vs.v).sum(axis=1).round()
          """))
      .project
    assert(
      get(p.vertexAttributes("s").runtimeSafeCast[Double]) ==
        Map(0 -> 61.0, 1 -> 55.0, 2 -> 151.0, 3 -> 6.0))
  }

  test("infer from code", SphynxOnly) {
    val p = box("Create example graph")
      .box(
        "Compute in Python",
        Map(
          "code" -> """
vs['with_title']: str = 'The Honorable ' + vs.name
vs['age_squared']: float = vs.age ** 2
es['score']: float = es.weight + es.comment.str.len()
es['names']: str = 'from ' + vs.name[es.src].values + ' to ' + vs.name[es.dst].values
graph_attributes.hello: str = graph_attributes.greeting.lower()
graph_attributes.average_age: float = vs.age.mean()
          """.trim),
      )
      .project
    assert(
      get(p.vertexAttributes("with_title").runtimeSafeCast[String]) ==
        Map(
          0 -> "The Honorable Adam",
          1 -> "The Honorable Eve",
          2 -> "The Honorable Bob",
          3 -> "The Honorable Isolated Joe"))
    assert(
      get(p.vertexAttributes("age_squared").runtimeSafeCast[Double]).mapValues(_.round) ==
        Map(0 -> 412, 1 -> 331, 2 -> 2530, 3 -> 4))
    assert(
      get(p.edgeAttributes("score").runtimeSafeCast[Double]) ==
        Map(0 -> 15.0, 1 -> 16.0, 2 -> 18.0, 3 -> 17.0))
    assert(
      get(p.edgeAttributes("names").runtimeSafeCast[String]) ==
        Map(0 -> "from Adam to Eve", 1 -> "from Eve to Adam", 2 -> "from Bob to Adam", 3 -> "from Bob to Eve"))
    assert(get(p.scalars("hello").runtimeSafeCast[String]) == "hello world! 😀 ")
    assert(get(p.scalars("average_age").runtimeSafeCast[Double]).round == 23)
  }

  test("table to table", SphynxOnly) {
    val t = box("Create example graph")
      .box("SQL1", Map("sql" -> "select name, age from vertices"))
      .box(
        "Compute in Python",
        Map(
          "inputs" -> "<infer from code>",
          "outputs" -> "<infer from code>",
          "code" -> """
df['age'] = np.round(df.age)
df['age_2']: float = df.age ** 2
          """),
      )
      // A second box to test that tables created in Python can also be used in Python.
      .box(
        "Compute in Python",
        Map(
          "inputs" -> "<infer from code>",
          // Let's also test that a different order of columns is not a problem.
          "outputs" -> "df.age_4: float, df.name: str",
          "code" -> """
df['age_4']: float = df.age_2 ** 2
          """,
        ),
      )
      .box("SQL1")
      .table
    val data = t.df.collect.toList.map(_.toSeq.toList)
    assert(data == List(
      List(160000.0, "Adam"),
      List(104976.0, "Eve"),
      List(6250000.0, "Bob"),
      List(16.0, "Isolated Joe")))
  }

  test("unsupported types are fine if you don't use them", SphynxOnly) {
    val p = box("Create example graph")
      .box("Train linear regression model", Map("features" -> "age"))
      .box("Compute in Python", Map("code" -> "vs['y']: str = vs.name"))
      .project
    assert(
      get(p.vertexAttributes("y").runtimeSafeCast[String]) ==
        Map(
          0 -> "Adam",
          1 -> "Eve",
          2 -> "Bob",
          3 -> "Isolated Joe"))
  }

  test("matplotlib with graph", SphynxOnly) {
    val html = box("Create example graph")
      .box(
        "Compute in Python",
        Map(
          "outputs" -> "matplotlib",
          "code" -> """
import matplotlib.pyplot as plt
plt.plot(vs['age'])
          """,
        ),
      )
      .output("graph").html
    // This is probably not enough of the SVG to confirm anything about the plot.
    // But if we produced some SVG then the rest is mostly in Matplotlib's hands.
    assert(get(html).startsWith("<img src=\"data:image/svg+xml;base64, PD94bWwgdmVyc2lv"))
  }

  test("matplotlib with table", SphynxOnly) {
    val html = box("Create example graph")
      .box("SQL1")
      .box(
        "Compute in Python",
        Map(
          "outputs" -> "matplotlib",
          "code" -> "df.plot()",
        ),
      )
      .output("graph").html
    // This is probably not enough of the SVG to confirm anything about the plot.
    // But if we produced some SVG then the rest is mostly in Matplotlib's hands.
    assert(get(html).startsWith("<img src=\"data:image/svg+xml;base64, PD94bWwgdmVyc2lv"))
  }

  test("html", SphynxOnly) {
    val html = box("Create example graph")
      .box(
        "Compute in Python",
        Map(
          "outputs" -> "html",
          "code" -> """
html = '<h1>hello world</h1>'
          """,
        ),
      )
      .output("graph").html
    assert(get(html) == "<h1>hello world</h1>")
  }
}
