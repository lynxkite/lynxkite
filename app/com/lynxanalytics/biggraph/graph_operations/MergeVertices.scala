package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Generates edges between vertices that match on an attribute.
object EdgesFromAttributeMatches {
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
}
import EdgesFromAttributeMatches._
case class EdgesFromAttributeMatches[T]() extends TypedMetaGraphOp[VertexAttributeInput[T], Output[T]] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    val partitioner = rc.defaultPartitioner
    val byAttr = inputs.attr.rdd.map { case (id, attr) => (attr, id) }
    val matching = byAttr.groupByKey(partitioner)
    val edges = matching.flatMap {
      case (attr, vertices) => for { a <- vertices; b <- vertices; if a != b } yield Edge(a, b)
    }
    output(o.edges, edges.randomNumbered(partitioner))
  }
}
