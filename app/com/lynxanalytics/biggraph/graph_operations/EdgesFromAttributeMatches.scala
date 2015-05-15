// Creates edges that connect vertices that have the same value for the given attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EdgesFromAttributeMatches extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = EdgesFromAttributeMatches()
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
    val attr = inputs.attr.rdd
    val byAttr = attr.map { case (id, attr) => (attr, id) }
    val matching = byAttr.groupByKey()
    val edges = matching.flatMap {
      case (attr, vertices) => for { a <- vertices; b <- vertices; if a != b } yield Edge(a, b)
    }
    output(o.edges, edges.randomNumbered(attr.partitioner.get))
  }
}

// Generates edges between vertices that match on an attribute.
// If fromAttr on A matches toAttr on B, an A -> B edge is generated.
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
  def fromJson(j: JsValue) = EdgesFromBipartiteAttributeMatches()
}
case class EdgesFromBipartiteAttributeMatches[T]()
    extends TypedMetaGraphOp[EdgesFromBipartiteAttributeMatches.Input[T], EdgesFromBipartiteAttributeMatches.Output[T]] {
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
    val fromByAttr = inputs.fromAttr.rdd
      .map { case (id, attr) => (attr, id) }
      .groupByKey()
    val toByAttr = inputs.toAttr.rdd
      .map { case (id, attr) => (attr, id) }
      .groupByKey()
    val edges = fromByAttr.join(toByAttr).flatMap {
      case (attr, (fromIds, toIds)) => for { a <- fromIds; b <- toIds } yield Edge(a, b)
    }
    val fromPartitioner = inputs.from.rdd.partitioner.get
    val toPartitioner = inputs.to.rdd.partitioner.get
    val partitioner =
      if (fromPartitioner.numPartitions > toPartitioner.numPartitions) fromPartitioner
      else toPartitioner
    output(o.edges, edges.randomNumbered(partitioner))
  }
}
