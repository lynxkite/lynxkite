package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// A small graph with all sorts of attributes. Used for testing.
object ExampleGraph extends OpFromJson {
  class Input extends MagicInputSignature {
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val (vertices, edges) = graph
    val name = vertexAttribute[String](vertices)
    val age = vertexAttribute[Double](vertices)
    val gender = vertexAttribute[String](vertices)
    val income = vertexAttribute[Double](vertices) // Partially defined.
    val comment = edgeAttribute[String](edges)
    val weight = edgeAttribute[Double](edges)
    val greeting = scalar[String]
    // For wholesale attribute access.
    val edgeAttributes = Map("comment" -> comment, "weight" -> weight)
    val vertexAttributes = Map("name" -> name, "age" -> age, "gender" -> gender, "income" -> income)
  }
  def fromJson(j: play.api.libs.json.JsValue) = ExampleGraph()
}
import ExampleGraph._
case class ExampleGraph() extends TypedMetaGraphOp[Input, Output] {
  @transient var executionCounter = 0

  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: ExampleGraph.Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    executionCounter += 1

    val sc = rc.sparkContext
    val partitioner = rc.onePartitionPartitioner
    output(
      o.vertices,
      sc.parallelize(Seq(0l, 1l, 2l, 3l).map((_, ())))
        .toSortedRDD(partitioner))
    output(
      o.edges,
      sc.parallelize(Seq(
        (0l, Edge(0l, 1l)),
        (1l, Edge(1l, 0l)),
        (2l, Edge(2l, 0l)),
        (3l, Edge(2l, 1l))))
        .toSortedRDD(partitioner))
    output(o.name, sc.parallelize(Seq(
      (0l, "Adam"),
      (1l, "Eve"),
      (2l, "Bob"),
      (3l, "Isolated Joe"))).toSortedRDD(partitioner))
    output(o.age, sc.parallelize(Seq(
      (0l, 20.3),
      (1l, 18.2),
      (2l, 50.3),
      (3l, 2.0))).toSortedRDD(partitioner))
    output(o.gender, sc.parallelize(Seq(
      (0l, "Male"),
      (1l, "Female"),
      (2l, "Male"),
      (3l, "Male"))).toSortedRDD(partitioner))
    output(o.income, sc.parallelize(Seq(
      (0l, 1000.0),
      (2l, 2000.0))).toSortedRDD(partitioner))
    output(o.comment, sc.parallelize(Seq(
      (0l, "Adam loves Eve"),
      (1l, "Eve loves Adam"),
      (2l, "Bob envies Adam"),
      (3l, "Bob loves Eve"))).toSortedRDD(partitioner))
    output(o.weight, sc.parallelize(Seq(
      (0l, 1.0),
      (1l, 2.0),
      (2l, 3.0),
      (3l, 4.0))).toSortedRDD(partitioner))
    output(o.greeting, "Hello world!")
  }
}
