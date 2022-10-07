package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.SphynxOnly
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class ComputeInRTest extends OperationsTestBase {
  test("example graph", SphynxOnly) {
    val p = box("Create example graph")
      .box(
        "Compute in R",
        Map(
          "inputs" -> "vs.name, vs.age, es.weight, es.comment, es.src, es.dst, graph_attributes.greeting",
          "outputs" -> "vs.with_title: character, vs.age_squared: double, es.score: double, es.names: character, graph_attributes.hello: character, graph_attributes.average_age: double",
          "code" -> """
vs <- vs %>%
  mutate(with_title = paste('The Honorable', name)) %>%
  mutate(age_squared = age ** 2)
es <- es %>%
  mutate(score = weight + nchar(comment)) %>%
  mutate(names = paste('from', vs$name[es$src], 'to', vs$name[es$dst]))
graph_attributes$hello <- tolower(graph_attributes$greeting)
graph_attributes$average_age <- mean(vs$age)
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
        "Compute in R",
        Map(
          "inputs" -> "vs.age",
          "outputs" -> "vs.v: vector",
          "code" -> """
vs <- vs %>%
  mutate(v = outer(age, c(1, 2)))
          """),
      )
      .box(
        "Compute in R",
        Map(
          "inputs" -> "vs.v",
          "outputs" -> "vs.s: double",
          "code" -> """
vs <- vs %>%
  rowwise() %>%
  mutate(s = round(sum(v)))
          """))
      .project
    assert(
      get(p.vertexAttributes("s").runtimeSafeCast[Double]) ==
        Map(0 -> 61.0, 1 -> 55.0, 2 -> 151.0, 3 -> 6.0))
  }

  test("table to table", SphynxOnly) {
    val t = box("Create example graph")
      .box("SQL1", Map("sql" -> "select name, age from vertices"))
      .box(
        "Compute in R",
        Map(
          "inputs" -> "df.age",
          "outputs" -> "df.age: double, df.age_2: double",
          "code" -> """
df <- df %>%
  mutate(age = round(age)) %>%
  mutate(age_2 = age ** 2)
          """,
        ),
      )
      // A second box to test that tables created in R can also be used in R.
      .box(
        "Compute in R",
        Map(
          "inputs" -> "df.age_2",
          // Let's also test that a different order of columns is not a problem.
          "outputs" -> "df.age_4: double, df.name: character",
          "code" -> """
df <- df %>% mutate(age_4 = age_2 ** 2)
          """,
        ),
      )
      .box("SQL1", Map("sql" -> "select age_4, name from input"))
      .table
    val data = t.df.collect.toList.map(_.toSeq.toList)
    assert(data == List(
      List(160000.0, "Adam"),
      List(104976.0, "Eve"),
      List(6250000.0, "Bob"),
      List(16.0, "Isolated Joe")))
  }

  test("create graph", SphynxOnly) {
    val p = box(
      "Create graph in R",
      Map(
        "outputs" -> "vs.name: character, es.weight: double, graph_attributes.hello: character",
        "code" -> """
vs <- tibble(
  name =  c('Alice', 'Bob', 'Cecil', 'Drew')
)
es <- tibble(
  src = c(1, 2, 3),
  dst = c(1, 3, 2),
  weight = c(1, 2, 3)
)
graph_attributes$hello <- 'hello'
          """,
      ),
    )
      .box("Compute degree", Map("direction" -> "all edges"))
      .project
    assert(
      get(p.vertexAttributes("name").runtimeSafeCast[String]) ==
        Map(0 -> "Alice", 1 -> "Bob", 2 -> "Cecil", 3 -> "Drew"))
    assert(
      get(p.vertexAttributes("degree").runtimeSafeCast[Double]) ==
        Map(0 -> 2, 1 -> 2, 2 -> 2, 3 -> 0))
    assert(
      get(p.edgeAttributes("weight").runtimeSafeCast[Double]) ==
        Map(0 -> 1, 1 -> 2, 2 -> 3))
    assert(get(p.scalars("hello").runtimeSafeCast[String]) == "hello")
  }
}
