// A small graph with all sorts of attributes. Used for testing.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ExampleGraph extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val (vertices, edges) = graph
    val name = vertexAttribute[String](vertices)
    val age = vertexAttribute[Double](vertices)
    val gender = vertexAttribute[String](vertices)
    val income = vertexAttribute[Double](vertices) // Partially defined.
    val location = vertexAttribute[(Double, Double)](vertices)
    val comment = edgeAttribute[String](edges)
    val weight = edgeAttribute[Double](edges)
    val greeting = scalar[String]
    // For wholesale access.
    val scalars = Map("greeting" -> greeting)
    val edgeAttributes = Map("comment" -> comment, "weight" -> weight)
    val vertexAttributes = Map(
      "name" -> name,
      "age" -> age,
      "gender" -> gender,
      "income" -> income,
      "location" -> location)
  }
  def fromJson(j: JsValue) = ExampleGraph()
}
import ExampleGraph._
case class ExampleGraph() extends TypedMetaGraphOp[NoInput, Output] {
  @transient var executionCounter = 0

  override val isHeavy = true
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet,
              o: ExampleGraph.Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    executionCounter += 1

    val sc = rc.sparkContext
    val partitioner = rc.onePartitionPartitioner
    output(
      o.vertices,
      sc.parallelize(Seq(0L, 1L, 2L, 3L).map((_, ())))
        .sortUnique(partitioner))
    output(
      o.edges,
      sc.parallelize(Seq(
        (0L, Edge(0L, 1L)),
        (1L, Edge(1L, 0L)),
        (2L, Edge(2L, 0L)),
        (3L, Edge(2L, 1L))))
        .sortUnique(partitioner))
    output(o.name, sc.parallelize(Seq(
      (0L, "Adam"),
      (1L, "Eve"),
      (2L, "Bob"),
      (3L, "Isolated Joe"))).sortUnique(partitioner))
    output(o.age, sc.parallelize(Seq(
      (0L, 20.3),
      (1L, 18.2),
      (2L, 50.3),
      (3L, 2.0))).sortUnique(partitioner))
    output(o.gender, sc.parallelize(Seq(
      (0L, "Male"),
      (1L, "Female"),
      (2L, "Male"),
      (3L, "Male"))).sortUnique(partitioner))
    output(o.income, sc.parallelize(Seq(
      (0L, 1000.0),
      (2L, 2000.0))).sortUnique(partitioner))
    output(o.location, sc.parallelize(Seq(
      (0L, (40.71448, -74.00598)), // New York
      (1L, (47.5269674, 19.0323968)), // Budapest
      (2L, (1.352083, 103.819836)), // Singapore
      (3L, (-33.8674869, 151.2069902)) // Sydney
    )).sortUnique(partitioner))
    output(o.comment, sc.parallelize(Seq(
      (0L, "Adam loves Eve"),
      (1L, "Eve loves Adam"),
      (2L, "Bob envies Adam"),
      (3L, "Bob loves Eve"))).sortUnique(partitioner))
    output(o.weight, sc.parallelize(Seq(
      (0L, 1.0),
      (1L, 2.0),
      (2L, 3.0),
      (3L, 4.0))).sortUnique(partitioner))
    output(o.greeting, "Hello world! 😀 ")
  }
}
