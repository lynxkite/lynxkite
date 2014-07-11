package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.graph_api._

// A small graph with all sorts of attributes. Used for testing.
object ExampleGraph {
  class Output(instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val (vertices, edges) = graph
    val name = vertexAttribute[String](vertices)
    val age = vertexAttribute[Double](vertices)
    val comment = edgeAttribute[String](edges)
    val weight = edgeAttribute[Double](edges)
    val greeting = scalar[String]
  }
}
case class ExampleGraph() extends TypedMetaGraphOp[NoInputProvider, ExampleGraph.Output] {
  @transient var executionCounter = 0

  override def inputSig = SimpleInputSignature()

  def result(instance: MetaGraphOperationInstance) = new ExampleGraph.Output(instance)

  def execute(inputDatas: DataSet,
              o: ExampleGraph.Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    executionCounter += 1

    val sc = rc.sparkContext
    output(
      o.vertices,
      sc.parallelize(Seq(0l, 1l, 2l, 3l).map((_, ())))
        .partitionBy(rc.onePartitionPartitioner))
    output(
      o.edges,
      sc.parallelize(Seq(
        (0l, Edge(0l, 1l)),
        (1l, Edge(1l, 0l)),
        (2l, Edge(2l, 0l)),
        (3l, Edge(2l, 1l))))
        .partitionBy(rc.onePartitionPartitioner))
    output(o.name, sc.parallelize(Seq(
      (0l, "Adam"),
      (1l, "Eve"),
      (2l, "Bob"),
      (3l, "Isolated Joe"))).partitionBy(rc.onePartitionPartitioner))
    output(o.age, sc.parallelize(Seq(
      (0l, 20.3),
      (1l, 18.2),
      (2l, 50.3),
      (3l, 2.0))).partitionBy(rc.onePartitionPartitioner))
    output(o.comment, sc.parallelize(Seq(
      (0l, "Adam loves Eve"),
      (1l, "Eve loves Adam"),
      (2l, "Bob envies Adam"),
      (3l, "Bob loves Eve"))).partitionBy(rc.onePartitionPartitioner))
    output(o.weight, sc.parallelize(Seq(
      (0l, 1.0),
      (1l, 2.0),
      (2l, 3.0),
      (3l, 4.0))).partitionBy(rc.onePartitionPartitioner))
    output(o.greeting, "Hello world!")
  }
}
