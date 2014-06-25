package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.graph_api._

// A small graph with all sorts of attributes. Used for testing.
case class ExampleGraph() extends MetaGraphOperation {
  @transient var executionCounter = 0

  def signature = newSignature
    .outputGraph('vertices, 'edges)
    .outputVertexAttribute[String]('name, 'vertices)
    .outputVertexAttribute[Double]('age, 'vertices)
    .outputEdgeAttribute[String]('comment, 'edges)
    .outputEdgeAttribute[Double]('weight, 'edges)
    .outputScalar[String]('greeting)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    executionCounter += 1

    val sc = rc.sparkContext
    outputs.putVertexSet(
      'vertices,
      sc.parallelize(Seq(0l, 1l, 2l).map((_, ())))
        .partitionBy(rc.onePartitionPartitioner))
    outputs.putEdgeBundle(
      'edges,
      sc.parallelize(Seq(
        (0l, Edge(0l, 1l)),
        (1l, Edge(1l, 0l)),
        (2l, Edge(2l, 0l)),
        (3l, Edge(2l, 1l))))
        .partitionBy(rc.onePartitionPartitioner))
    outputs.putVertexAttribute[String]('name, sc.parallelize(Seq(
      (0l, "Adam"),
      (1l, "Eve"),
      (2l, "Bob"))).partitionBy(rc.onePartitionPartitioner))
    outputs.putVertexAttribute[Double]('age, sc.parallelize(Seq(
      (0l, 20.3),
      (1l, 18.2),
      (2l, 50.3))).partitionBy(rc.onePartitionPartitioner))
    outputs.putEdgeAttribute[String]('comment, sc.parallelize(Seq(
      (0l, "Adam loves Eve"),
      (1l, "Eve loves Adam"),
      (2l, "Bob envies Adam"),
      (3l, "Bob loves Eve"))).partitionBy(rc.onePartitionPartitioner))
    outputs.putEdgeAttribute[Double]('weight, sc.parallelize(Seq(
      (0l, 1.0),
      (1l, 2.0),
      (2l, 3.0),
      (3l, 4.0))).partitionBy(rc.onePartitionPartitioner))
    outputs.putScalar('greeting, "Hello world!")
  }
}
