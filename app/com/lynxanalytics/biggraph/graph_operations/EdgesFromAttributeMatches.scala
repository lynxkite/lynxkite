package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Generates edges between vertices that match on an attribute.
object EdgesFromAttributeMatches extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = EdgesFromAttributeMatches[Any]()
}
case class EdgesFromAttributeMatches[T]() extends TypedMetaGraphOp[VertexAttributeInput[T], EdgesFromAttributeMatches.Output[T]] {
  import EdgesFromAttributeMatches._
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

// Generates edges between vertices that match on an attribute.
object EdgesFromBipartiteAttributeMatches extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val from = vertexSet
    val to = vertexSet
    val fromAttr = vertexAttribute[T](from)
    val toAttr = vertexAttribute[T](to)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: Input[T])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.from.entity, inputs.to.entity)
  }
  def fromJson(j: JsValue) = EdgesFromBipartiteAttributeMatches[Any]()
}
case class EdgesFromBipartiteAttributeMatches[T]() extends TypedMetaGraphOp[EdgesFromBipartiteAttributeMatches.Input[T], EdgesFromBipartiteAttributeMatches.Output[T]] {
  import EdgesFromBipartiteAttributeMatches._
  override val isHeavy = true
  @transient override lazy val inputs = new Input[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.fromAttr.data.classTag
    val partitioner = rc.defaultPartitioner
    val fromByAttr = inputs.fromAttr.rdd
      .map { case (id, attr) => (attr, id) }
      .groupByKey(partitioner)
    val toByAttr = inputs.toAttr.rdd
      .map { case (id, attr) => (attr, id) }
      .groupByKey(partitioner)
    val edges = fromByAttr.join(toByAttr).flatMap {
      case (attr, (fromIds, toIds)) => for { a <- fromIds; b <- toIds } yield Edge(a, b)
    }
    output(o.edges, edges.randomNumbered(partitioner))
  }
}
