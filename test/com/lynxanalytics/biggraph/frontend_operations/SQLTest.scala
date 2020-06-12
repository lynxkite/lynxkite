package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.SQLTestCases
import com.lynxanalytics.biggraph.graph_api.{ DataManager, SparkDomain, ThreadUtil }
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_util.ControlledFutures
import org.apache.spark
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

object SQLTest {
  def toSeq(row: spark.sql.Row): Seq[Any] = {
    row.toSeq.map {
      case r: spark.sql.Row => toSeq(r)
      case x => x
    }
  }
}
import SQLTest._
class SQLTest extends OperationsTestBase {
  private def runQueryOnExampleGraph(sql: String) = {
    box("Create example graph").box("SQL1", Map("sql" -> sql)).table
  }
  test("java_method is disabled") {
    val c = "java.net.InetAddress"
    val m = "getLocalHost"
    intercept[Throwable] {
      runQueryOnExampleGraph(s"select java_method('$c', '$m') from vertices").df.collect()
    }
    intercept[Throwable] {
      runQueryOnExampleGraph(s"select java_method() from vertices").df.collect()
    }
  }

  test("vertices table") {
    val table = runQueryOnExampleGraph("select * from vertices order by id")
    assert(table.schema.map(_.name) == Seq("age", "gender", "id", "income", "location", "name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq(20.3, "Male", "0", 1000.0, Seq(40.71448, -74.00598), "Adam"),
      Seq(18.2, "Female", "1", null, Seq(47.5269674, 19.0323968), "Eve"),
      Seq(50.3, "Male", "2", 2000.0, Seq(1.352083, 103.819836), "Bob"),
      Seq(2.0, "Male", "3", null, Seq(-33.8674869, 151.2069902), "Isolated Joe")))
  }

  test("edges table") {
    val table = runQueryOnExampleGraph("select * from edges order by edge_comment")
    assert(table.schema.map(_.name) == Seq("dst_age", "dst_gender", "dst_id", "dst_income",
      "dst_location", "dst_name", "edge_comment", "edge_weight", "src_age", "src_gender", "src_id",
      "src_income", "src_location", "src_name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq(18.2, "Female", "1", null, Seq(47.5269674, 19.0323968), "Eve", "Adam loves Eve", 1.0,
        20.3, "Male", "0", 1000.0, Seq(40.71448, -74.00598), "Adam"),
      Seq(20.3, "Male", "0", 1000.0, Seq(40.71448, -74.00598), "Adam", "Bob envies Adam", 3.0, 50.3,
        "Male", "2", 2000.0, Seq(1.352083, 103.819836), "Bob"),
      Seq(18.2, "Female", "1", null, Seq(47.5269674, 19.0323968), "Eve", "Bob loves Eve", 4.0, 50.3,
        "Male", "2", 2000.0, Seq(1.352083, 103.819836), "Bob"),
      Seq(20.3, "Male", "0", 1000.0, Seq(40.71448, -74.00598), "Adam", "Eve loves Adam", 2.0, 18.2,
        "Female", "1", null, Seq(47.5269674, 19.0323968), "Eve")))
  }

  test("edge_attributes table") {
    val table = runQueryOnExampleGraph("select * from edge_attributes order by comment")
    assert(table.schema.map(_.name) == Seq("comment", "weight"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam loves Eve", 1.0),
      Seq("Bob envies Adam", 3.0),
      Seq("Bob loves Eve", 4.0),
      Seq("Eve loves Adam", 2.0)))
  }

  test("belongs_to table") {
    val table = box("Create example graph")
      .box("Find connected components")
      .box("SQL1", Map("sql" -> """
        select base_name, segment_id, segment_size
        from `connected_components.belongs_to` order by base_id"""))
      .table
    assert(table.schema.map(_.name) == Seq("base_name", "segment_id", "segment_size"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam", "0", 3.0), Seq("Eve", "0", 3.0), Seq("Bob", "0", 3.0), Seq("Isolated Joe", "3", 1.0)))
  }

  test("scalars table") {
    val table = runQueryOnExampleGraph("select `!edge_count`, `!vertex_count` from graph_attributes")
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(table.schema.map(_.name) == Seq("!edge_count", "!vertex_count"))
    assert(data == Seq(Seq(4.0, 4.0)))
  }

  test("scalars table different column order") {
    val table = runQueryOnExampleGraph(
      "select greeting, `!vertex_count`, `!edge_count` from graph_attributes")
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(table.schema.map(_.name) == Seq("greeting", "!vertex_count", "!edge_count"))
    assert(data == Seq(Seq("Hello world! 😀 ", 4.0, 4.0)))
  }

  test("functions") {
    val table = runQueryOnExampleGraph("select avg(age) as avg_age from vertices")
    assert(table.schema.map(_.name) == Seq("avg_age"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(Seq(22.7)))
  }

  test("sql on vertices") {
    val table = runQueryOnExampleGraph("select name from vertices where age < 40")
    assert(table.schema.map(_.name) == Seq("name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("sql with empty results") {
    val table = runQueryOnExampleGraph("select id from vertices where id = 11")
    assert(table.schema.map(_.name) == Seq("id"))
    val data = table.df.collect.toSeq
    assert(data == List())
  }

  test("sql file reading is disabled") {
    val file = getClass.getResource("/controllers/noread.csv").toString
    val ws = box("Create example graph")
      .box("SQL1", Map("sql" -> s"select * from csv.`$file`"))
    intercept[AssertionError] {
      ws.table
    }
  }

  test("three inputs") {
    val one = box("Create example graph")
    val two = box("Create example graph")
    val three = box("Create example graph")
    val table = box("SQL3", Map("sql" -> """
      select one.edge_comment, two.name as src_name, three.name as dst_name
      from `one.edges` as one
      join `two.vertices` as two
      join `three.vertices` as three
      where one.src_name = two.name and one.dst_name = three.name
      """), Seq(one, two, three)).table
    assert(table.schema.map(_.name) == Seq("edge_comment", "src_name", "dst_name"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Bob envies Adam", "Bob", "Adam"),
      Seq("Eve loves Adam", "Eve", "Adam"),
      Seq("Adam loves Eve", "Adam", "Eve"),
      Seq("Bob loves Eve", "Bob", "Eve")))
  }

  test("the order of the input mapping does not matter") {
    val eg = box("Create example graph")
    val one = eg.box("SQL1", Map("sql" -> "select 'one' as table"))
    val two = eg.box("SQL1", Map("sql" -> "select 'two' as table"))
    val table = box(
      "SQL2", Map("sql" -> "select * from one"),
      inputMap = collection.immutable.ListMap("two" -> two, "one" -> one)).table
    assert(table.df.head.get(0) == "one")
  }

  test("union") {
    val one = box("Create example graph")
    val two = box("Create example graph")
    val table = box("SQL2", Map("sql" -> """
      select * from (select edge_comment
      from `one.edges`
      union all
      select edge_comment
      from `two.edges`)
      order by edge_comment
      """), Seq(one, two)).table
    assert(table.schema.map(_.name) == Seq("edge_comment"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam loves Eve"),
      Seq("Adam loves Eve"),
      Seq("Bob envies Adam"),
      Seq("Bob envies Adam"),
      Seq("Bob loves Eve"),
      Seq("Bob loves Eve"),
      Seq("Eve loves Adam"),
      Seq("Eve loves Adam")))
  }

  test("union and sum of counts") {
    val one = box("Create example graph")
    val two = box("Create example graph")
    val table = box("SQL2", Map("sql" -> """
      select sum(cnt) as cnt from (
      select count(age) as cnt from `one.vertices`
      union all
      select count(income) as cnt from `two.vertices`
      )
      """), Seq(one, two)).table
    assert(table.schema.map(_.name) == Seq("cnt"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(Seq(6)))
  }

  test("group by") {
    val table = runQueryOnExampleGraph("select id, count(*) as count from vertices group by id")
    assert(table.schema.map(_.name) == Seq("id", "count"))
  }

  test("no group by count(*)") {
    val table = runQueryOnExampleGraph("select count(*) as Sum from vertices")
    assert(table.schema.map(_.name) == Seq("Sum"))
  }

  test("subquery") {
    val table = runQueryOnExampleGraph("select id from (select * from vertices) as sub")
    assert(table.schema.map(_.name) == Seq("id"))
  }

  test("no table") {
    val table = runQueryOnExampleGraph("select 1 as one, int(null) as n")
    assert(table.schema.map(_.name) == Seq("one", "n"))
  }

  test("missing column") {
    intercept[AssertionError] {
      val table = box("Create example graph")
        .box("SQL1", Map("sql" -> "select nonexistent from vertices"))
        .table
    } match {
      case a: AssertionError =>
        assert(a.getMessage.contains("column cannot be found"))
    }
  }

  test("alias") {
    val table = runQueryOnExampleGraph("select v.age from vertices v")
    assert(table.schema.map(_.name) == Seq("age"))
  }

  test("name reference") {
    val table = runQueryOnExampleGraph("select vertices.age from vertices")
    assert(table.schema.map(_.name) == Seq("age"))
  }

  test("prefixed table name even with one input") {
    val table = runQueryOnExampleGraph("select age from `input.vertices`")
    assert(table.schema.map(_.name) == Seq("age"))
  }

  test("no edge attributes") {
    val noEdges = box("Create example graph")
      .box("Discard edge attributes", Map("name" -> "comment,weight"))
    assert(noEdges.project.edgeAttributes.size == 0)
    val table = noEdges
      .box("SQL1", Map("sql" -> "select * from vertices"))
      .table
    assert(table.schema.map(_.name) == Seq("age", "gender", "id", "income", "location", "name"))
  }

  test("set type attribute") {
    val table = box("Create example graph")
      .box("Aggregate edge attribute to vertices", Map(
        "prefix" -> "edge",
        "direction" -> "all edges",
        "aggregate_weight" -> "set"))
      .box("SQL1", Map("sql" -> "select edge_weight_set from vertices", "persist" -> "no"))
      .table
    val data = table.df.collect.toSeq.map(row => {
      val asSeq = row.toSeq(0).asInstanceOf[Seq[Double]]
      if (asSeq == null) {
        null
      } else {
        asSeq.sorted
      }
    })
    assert(table.schema.map(_.name) == Seq("edge_weight_set"))
    assert(data == Seq(
      Seq(1.0, 2.0, 3.0),
      Seq(1.0, 2.0, 4.0),
      Seq(3.0, 4.0),
      null))
  }

  test("Thread safe sql even for the same SQL context") {
    val columnName = "col"
    val tableName = "table"
    val schema = StructType(Seq(StructField("column", IntegerType)))
    def getTinyDfWithConstantValue(ctx: SQLContext, value: Int): DataFrame = {
      import scala.collection.JavaConverters._
      val data = List(Row(value)).asJava
      ctx.createDataFrame(data, schema).toDF(columnName)
    }

    val numWorkers = 60
    val maxParalellism = 20
    implicit val sqlTestExecutionContext =
      ThreadUtil.limitedExecutionContext("ThreadSafeSQL", maxParallelism = maxParalellism)
    val sqlTestingThreads = new ControlledFutures()(sqlTestExecutionContext)

    // Ideally, worker w should only write resultPoolForEachWorker(w), thus
    // resultPoolForEachWorker should contain only 1's, since each worker runs
    // once. But this is not the case when the race condition hits, then workers
    // begin to use each other's data frames.
    val resultPoolForEachWorker = Array.fill(numWorkers)(new java.util.concurrent.atomic.AtomicInteger(0))

    val sqlCtx = sparkDomain.newSQLContext
    for (workerId <- (0 until numWorkers)) {
      val df = List((tableName, getTinyDfWithConstantValue(sqlCtx, workerId)))
      sqlTestingThreads.register {
        val resultThatIsSupposedToBeEqualToWorkerId =
          SparkDomain.sql(sqlCtx, s"select AVG($columnName) from $tableName", df)
            .collect().toList.take(1).head.getDouble(0)
        val idx = resultThatIsSupposedToBeEqualToWorkerId.toInt
        // We could say assert(workerId == resultThatIsSupposedToBeEqualToWorkerId) here,
        // but this is running on a different thread using SafeFuture,
        // and the test would pass despite the assertion. Anyway, using the resultPoolForEachWorker array
        // gives us a nicer way to see what exactly went wrong.
        if (0 <= idx && idx < numWorkers) {
          resultPoolForEachWorker(idx).getAndIncrement()
        }
      }
    }

    sqlTestingThreads.waitAllFutures()
    val resultMap = (0 until numWorkers).map(n => (n, resultPoolForEachWorker(n).get()))
    val badResults = resultMap.filter(_._2 != 1).toList
    // Failure here would result in outputs like these:
    //
    // List((9,2), (10,0), (14,0), (16,2)) was not empty (SQLTest.scala:194)
    //
    // This would (probably) mean that worker 9 overwrote the data frame registered by
    // worker 10, and worker 16 overwrote the data frame registered by worker 14.
    // (All workers use the same name when registering their data frames.)
    assert(badResults.isEmpty)
  }

  test("no required attribute") {
    val table = runQueryOnExampleGraph(
      "select 1 from (select age, gender from vertices) limit 2")
    assert(table.df.collect.toSeq.map(row => toSeq(row)) == Seq(Seq(1), Seq(1)))
  }

  test("table reuse") {
    val select = (s: String) => s"(select $s from vertices limit 1)"
    val table = runQueryOnExampleGraph(s"${select("age")} union ${select("name")}")
    assert(table.df.collect.toSeq.map(row => toSeq(row)) == Seq(Seq("20.3"), Seq("Adam")))
  }

  test("multi-alias") {
    val table = runQueryOnExampleGraph(
      "select blah from (select age as blah from vertices) order by blah limit 2")
    assert(table.df.collect.toSeq.map(row => toSeq(row)) == Seq(Seq(2.0), Seq(18.2)))
  }

  test("same guid for same sequence") {
    val x = runQueryOnExampleGraph("select * from vertices")
    val y = runQueryOnExampleGraph("select * from vertices")
    assert(x.gUID == y.gUID)
  }

  test("user defined functions - hash") {
    val t = runQueryOnExampleGraph("select hash(name, 'salt') from vertices")
    assert(t.df.collect.toSeq.map(row => toSeq(row)) == Seq(
      Seq("22B5B6740450B367DEDADC0EF5142F42E3BBAFAA19F8FDA1016E3DA0389CB0D8"),
      Seq("FE3DA8E41069BE8068ACCD8AA32788D1037898114CBF213E851BD1DCDD15F092"),
      Seq("11D690C14A747559DFFF9B63ACA0F894F338D3EEBEACCBDF7C6FFCABAEEAF6F0"),
      Seq("33D6D5B65AC19814812586FAF9D1399DFB70D16EEB66702B62AB001B38CE3259")))
  }

  test("user defined functions - geodistance") {
    val t = box("Create example graph")
      .box("Derive vertex attribute", Map("output" -> "lat", "expr" -> "location(0)"))
      .box("Derive vertex attribute", Map("output" -> "lon", "expr" -> "location(1)"))
      .box("SQL1", Map("sql" -> "select geodistance(src_lat, src_lon, dst_lat, dst_lon) from edges")).table
    assert(t.df.collect.toSeq.map(row => toSeq(row)) == Seq(
      Seq(7023993.307994274), // New York / Budapest ~7k kilometers.
      Seq(7023993.307994274),
      Seq(1.5340398666732997E7), // New York / Singapore ~15k kilometers.
      Seq(9507129.781908857))) // Budapest / Singapore ~ 9.5k kilometers.
  }

  test("user defined functions - dayofweek") {
    val t = runQueryOnExampleGraph("""select
      dayofweek('2018-01-01'),
      dayofweek('2018-01-06'),
      dayofweek('2018-01-07') from vertices""")
    val s = t.df.collect.toSeq
    assert(s(0).toSeq == Seq(2, 7, 1)) // Monday, Saturday, Sunday.
  }

  test("user defined functions - string_intersect") {
    val one = box("Create example graph").box(
      "SQL1",
      Map("sql" -> """select collect_set(name) as names
                      from `vertices`
                      where gender = 'Male'"""))
    val two = box("Create example graph").box(
      "SQL1",
      Map("sql" -> """select collect_set(name) as names
                      from `vertices`
                      where income > 0"""))
    val table = box("SQL2", Map("sql" -> """
      select string_intersect(one.names, two.names) as si
      from one cross join two
      """), Seq(one, two)).table
    assert(table.schema.map(_.name) == Seq("si"))
    val data = table.df.collect.toSeq.map(row => toSeq(row))
    def flatten(ls: Seq[Any]): Seq[Any] = ls flatMap {
      case i: Seq[_] => flatten(i)
      case e => Seq(e)
    }
    assert(flatten(data).toSet == Set("Adam", "Bob"))
  }

  test("user defined aggr functions - most_common") {
    val t = runQueryOnExampleGraph("select most_common(gender) from vertices")
    assert(t.df.collect.toSeq.map(row => toSeq(row)) == Seq(Seq("Male")))
  }

  test("user defined aggr functions - most_common with null values") {
    val query = "select gender, most_common(income) from vertices group by gender"
    val t = runQueryOnExampleGraph(query)
    assert(t.df.collect.toSeq.map(
      row => toSeq(row)) == Seq(Seq("Female", null), Seq("Male", "2000.0")))
  }

  test("columns in tuples are nullable") {
    // With persist = "yes" the result is saved as parquet.
    // To be able to use it in later operations, all columns have to
    // be nullable in the schema. Even inside tuples, like "location".
    val sql = "select name, location from input"
    val t = box("Create example graph")
      .box("SQL1", Map("sql" -> "select * from vertices", "persist" -> "yes"))
      .box("SQL1", Map("sql" -> sql))
      .table
    assert(t.df.collect.toSeq.map(
      row => toSeq(row)) == Seq(
        Seq("Adam", Seq(40.71448, -74.00598)),
        Seq("Eve", Seq(47.5269674, 19.0323968)),
        Seq("Bob", Seq(1.352083, 103.819836)),
        Seq("Isolated Joe", Seq(-33.8674869, 151.2069902))))
  }

  test("array columns can be persisted") {
    val t = box("Create example graph")
      .box("Compute degree", Map("direction" -> "incoming edges"))
      .box("Merge vertices by attribute", Map("key" -> "degree", "aggregate_age" -> "set"))
      .box("SQL1", Map("sql" -> "select * from vertices", "persist" -> "yes"))
      .box("SQL1", Map("sql" -> "select age_set from input"))
      .table
    assert(t.df.collect.toSeq.map(
      row => toSeq(row)) == Seq(
        Seq(Seq(20.3, 18.2)),
        Seq(Seq(50.3, 2.0))))
  }

  test("nested array columns can be persisted") {
    val t = box("Create example graph")
      .box("Compute degree", Map("direction" -> "incoming edges"))
      .box("Merge vertices by attribute", Map("key" -> "degree", "aggregate_age" -> "vector"))
      .box("Add constant vertex attribute", Map("name" -> "c"))
      .box("Merge vertices by attribute", Map("key" -> "c", "aggregate_age_vector" -> "vector"))
      .box("SQL1", Map("sql" -> "select * from vertices", "persist" -> "yes"))
      .box("SQL1", Map("sql" -> "select age_vector_vector, c from input"))
      .table
    assert(t.df.collect.toSeq.map(
      row => toSeq(row)) == Seq(Seq(Seq(Seq(20.3, 18.2), Seq(50.3, 2.0)), 1)))
  }

  test("inputs can be given custom names") {
    val eg = box("Create example graph")
    val t = box("SQL2", Map(
      "sql" -> "select count(*) as cnt from `a.vertices` join `b.vertices` using (name)",
      "input_names" -> "a, b"), Seq(eg, eg)).table
    assert(t.df.collect.toSeq.map(toSeq) == Seq(Seq(4)))
  }

  SQLTestCases.list.foreach(query => test(query._1) {
    val one = box("Create example graph")
    val two = box("Create example graph")
    val graphQuery = query._1.replace("one", "`one.vertices`").replace("two", "`two.vertices`")
    box("SQL2", Map("sql" -> graphQuery), Seq(one, two)).table.df.collect()
  })
}
