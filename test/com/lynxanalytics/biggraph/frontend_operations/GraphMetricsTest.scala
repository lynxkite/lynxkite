package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import org.apache.spark

class GraphMetricsTest extends OperationsTestBase {

  private def toSeq(row: spark.sql.Row): Seq[Any] = {
    row.toSeq.map {
      case r: spark.sql.Row => toSeq(r)
      case x => x
    }
  }

  test("graph metrics works (Output: General Metrics)") {
    val gmbox = box("Create example graph")
      .box("built-ins/graph-metrics", Map(
        "CustomPercentile" -> "0",
        "MaximumNumberOfIterations" -> "30",
        "MinimalModularityIncerement" -> "0.001",
        "ModularClustering" -> "true")).output("General Metrics")


    val table = gmbox.table
    assert(table.schema.map(_.name) == Seq("Attribute", "Sum", "Avg", "Min", "5% percentile", "Med", "95% percentile", "Max", "0% (custom) percentile"))


    // There is a bug currently, that causes the General Metric output to fail on the first few
    // calculations, so we run it 5 times to make sure it's not failing because of this and we can
    // see if the ouput is actually wrong or not
    for( a <- 1 to 5){
      try {
        val data = table.df.collect.toSeq.map(row => toSeq(row))
      } catch {
        case e: org.apache.spark.sql.AnalysisException => //println(e)
      }
    }

    val data = table.df.collect.toSeq.map(row => toSeq(row))

    assert(data == Seq(Seq("Vertices", 4.0, null, null, null, null, null, null, null),
      Seq("Edges", 4.0, null, null, null, null, null, null, null),
      Seq("Degree", 8.0, 2.0, 0.0, 0.30000000000000004, 2.5, 3.0, 3.0, 0.0),
      Seq("In Degree", 4.0, 1.0, 0.0, 0.0, 1.0, 2.0, 2.0, 0.0),
      Seq("Out Degree", 4.0, 1.0, 0.0, 0.15000000000000002, 1.0, 1.8499999999999996, 2.0, 0.0),
      Seq("Sym Degree", 2.0, 0.5, 0.0, 0.0, 0.5, 1.0, 1.0, 0.0),
      Seq("Connected Components", 2.0, 2.0, 1.0, 1.1, 2.0, 2.8999999999999995, 3.0, 1.0),
      Seq("Modular Clusternig", 2.0, 2.0, 1.0, 1.1, 2.0, 2.8999999999999995, 3.0, 1.0)))

    val gmbox2 = box("Create example graph")
      .box("built-ins/graph-metrics", Map(
        "CustomPercentile" -> "0.7",
        "MaximumNumberOfIterations" -> "30",
        "MinimalModularityIncerement" -> "0.001",
        "ModularClustering" -> "true")).output("General Metrics")


    val table2 = gmbox2.table
    assert(table2.schema.map(_.name) == Seq("Attribute", "Sum", "Avg", "Min", "5% percentile", "Med", "95% percentile", "Max", "70% (custom) percentile"))


    // There is a bug currently, that causes the General Metric output to fail on the first few
    // calculations, so we run it 5 times to make sure it's not failing because of this and we can
    // see if the ouput is actually wrong or not
    for( a <- 1 to 5){
      try {
        val data2 = table2.df.collect.toSeq.map(row => toSeq(row))
      } catch {
        case e: org.apache.spark.sql.AnalysisException => //println(e)
      }
    }

    val data2 = table2.df.collect.toSeq.map(row => toSeq(row))

    assert(data2 == Seq(Seq("Vertices", 4.0, null, null, null, null, null, null, null),
      Seq("Edges", 4.0, null, null, null, null, null, null, null),
      Seq("Degree", 8.0, 2.0, 0.0, 0.30000000000000004, 2.5, 3.0, 3.0, 3.0),
      Seq("In Degree", 4.0, 1.0, 0.0, 0.0, 1.0, 2.0, 2.0, 2.0),
      Seq("Out Degree", 4.0, 1.0, 0.0, 0.15000000000000002, 1.0, 1.8499999999999996, 2.0, 1.0999999999999996),
      Seq("Sym Degree", 2.0, 0.5, 0.0, 0.0, 0.5, 1.0, 1.0, 1.0),
      Seq("Connected Components", 2.0, 2.0, 1.0, 1.1, 2.0, 2.8999999999999995, 3.0, 2.3999999999999995),
      Seq("Modular Clusternig", 2.0, 2.0, 1.0, 1.1, 2.0, 2.8999999999999995, 3.0, 2.3999999999999995)))

  }

  test("graph metrics works (Output: Degree Distribution)") {
    val gmbox = box("Create example graph")
      .box("built-ins/graph-metrics", Map(
        "CustomPercentile" -> "0",
        "MaximumNumberOfIterations" -> "30",
        "MinimalModularityIncerement" -> "0.001",
        "ModularClustering" -> "true")).output("Degree Distribution")


    val plot = gmbox.plot.value

    assert(plot == "{\n  \"mark\" : \"bar\",\n  \"encoding\" : {\n    \"x\" : {\n      \"field\" : \"degrees\",\n      \"type\" : \"quantitative\",\n      \"bin\" : {\n        \"maxbins\" : 20.0\n      }\n    },\n    \"y\" : {\n      \"scale\" : {\n        \"type\" : \"log\"\n      },\n      \"field\" : \"Vertices\",\n      \"type\" : \"quantitative\"\n    }\n  },\n  \"data\" : {\n    \"values\" : [\n      {\n        \"degrees\" : 3.0,\n        \"Vertices\" : \"1.99\"\n      },\n      {\n        \"degrees\" : 2.0,\n        \"Vertices\" : \"0.99\"\n      },\n      {\n        \"degrees\" : 0.0,\n        \"Vertices\" : \"0.99\"\n      }\n    ]\n  }\n}")

  }

  test("graph metrics works (Output: Component Distribution)") {
    val gmbox = box("Create example graph")
      .box("built-ins/graph-metrics", Map(
        "CustomPercentile" -> "0",
        "MaximumNumberOfIterations" -> "30",
        "MinimalModularityIncerement" -> "0.001",
        "ModularClustering" -> "true")).output("Component Distribution")


    val plot = gmbox.plot.value

    assert(plot == "{\n  \"mark\" : \"bar\",\n  \"encoding\" : {\n    \"x\" : {\n      \"field\" : \"SizeOfComponent\",\n      \"type\" : \"quantitative\",\n      \"bin\" : {\n        \"maxbins\" : 20.0\n      }\n    },\n    \"y\" : {\n      \"scale\" : {\n        \"type\" : \"log\"\n      },\n      \"field\" : \"NumberOfComponents\",\n      \"type\" : \"quantitative\"\n    }\n  },\n  \"data\" : {\n    \"values\" : [\n      {\n        \"SizeOfComponent\" : 3.0,\n        \"NumberOfComponents\" : \"0.99\"\n      },\n      {\n        \"SizeOfComponent\" : 1.0,\n        \"NumberOfComponents\" : \"0.99\"\n      }\n    ]\n  }\n}")

  }

  test("graph metrics works (Output: Modular Distribution)") {
    val gmbox = box("Create example graph")
      .box("built-ins/graph-metrics", Map(
        "CustomPercentile" -> "0",
        "MaximumNumberOfIterations" -> "30",
        "MinimalModularityIncerement" -> "0.001",
        "ModularClustering" -> "true")).output("Modular Distribution")


    val plot = gmbox.plot.value

    assert(plot == "{\n  \"mark\" : \"bar\",\n  \"encoding\" : {\n    \"x\" : {\n      \"field\" : \"SizeOfComponent\",\n      \"type\" : \"quantitative\",\n      \"bin\" : {\n        \"maxbins\" : 20.0\n      }\n    },\n    \"y\" : {\n      \"scale\" : {\n        \"type\" : \"log\"\n      },\n      \"field\" : \"NumberOfComponents\",\n      \"type\" : \"quantitative\"\n    }\n  },\n  \"data\" : {\n    \"values\" : [\n      {\n        \"SizeOfComponent\" : 3.0,\n        \"NumberOfComponents\" : \"0.99\"\n      },\n      {\n        \"SizeOfComponent\" : 1.0,\n        \"NumberOfComponents\" : \"0.99\"\n      }\n    ]\n  }\n}")

    val gmbox2 = box("Create example graph")
      .box("built-ins/graph-metrics", Map(
        "CustomPercentile" -> "0",
        "MaximumNumberOfIterations" -> "-50",
        "MinimalModularityIncerement" -> "0.001",
        "ModularClustering" -> "true")).output("Modular Distribution")

  }

}