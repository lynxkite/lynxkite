package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

// Things tested here are unfortunately not trivial due to things not being registered in kryo...
class SparkSQLTest extends FunSuite with TestDataManager with BeforeAndAfter {

  test("We can run a simple SparkSQL workflow using our internal spark context") {
    val sqlContext = cleanDataManager.newSQLContext()
    val resDir = getClass.getResource("/graph_api/SparkSQLTest").toString
    val df = sqlContext.read.json(resDir + "/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 21).show()
    df.groupBy("age").count().show()
  }

  test("We can run do SQL on dataframes and reuse results as normal RDD") {
    val sqlContext = cleanDataManager.newSQLContext()
    val resDir = getClass.getResource("/graph_api/SparkSQLTest").toString
    val df = sqlContext.read.json(resDir + "/people.json")
    df.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    teenagers.show()
    import sqlContext.implicits._

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
  }

  test("We can create a DataFrame from a normal RDD and a programmatically created schema") {
    val sqlContext = cleanDataManager.newSQLContext()
    val resDir = getClass.getResource("/graph_api/SparkSQLTest").toString

    val people = sparkContext.textFile(resDir + "/people.txt")

    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        Seq(StructField("name", StringType, true), StructField("age", IntegerType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim.toInt))

    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    peopleDataFrame.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")

    import sqlContext.implicits._
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    results.map(t => "Name: " + t(0)).collect().foreach(println)
  }

  /*
  test("DataFrame from LynxKite project") {
    import com.lynxanalytics.biggraph.controllers._

    val env = new TestGraphOp {}
    implicit val mm = env.metaGraphManager
    val controller = new BigGraphController(env)
    val projectName = "df-test"
    val user = com.lynxanalytics.biggraph.serving.User.fake
    controller.createProject(
      user,
      CreateProjectRequest(name = projectName, notes = "test project", privacy = "private"))
    def run(op: String, params: Map[String, String] = Map(), on: String = projectName) = {
      controller.projectOp(
        user,
        ProjectOperationRequest(on, FEOperationSpec(op, params)))
    }
    val projectFrame = ProjectFrame.fromName(projectName)
    val subProject = projectFrame.subproject
    run("Example Graph", Map())
    // Add an attribute that is of a type that DataFrames do not support.
    run("Aggregate on neighbors",
      Map("prefix" -> "", "direction" -> "all edges", "aggregate-name" -> "set"))
    implicit val dm = env.dataManager
    val df = Table.fromTableName("vertices", subProject.viewer).toDF(dm.newSQLContext())
    df.printSchema()
    df.show()
  }
  */
}
