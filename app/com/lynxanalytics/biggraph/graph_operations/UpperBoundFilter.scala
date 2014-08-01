package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import scala.math.Ordering.Implicits._

object UpperBoundFilter {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T]) extends MagicOutput(instance) {
    val fvs = vertexSet
    val identity = edgeBundle(inputs.vs.entity, fvs)
  }
}
import UpperBoundFilter._
case class UpperBoundFilter[T: Ordering](bound: T)
    extends TypedMetaGraphOp[VertexAttributeInput[T], Output[T]] {
  @transient override lazy val inputs = new VertexAttributeInput[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val tt = inputs.attr.data.typeTag
    implicit val ct = inputs.attr.data.classTag
    val attr = inputs.attr.rdd
    val fattr = attr.filter { case (id, v) => v <= bound }
    output(o.fvs, fattr.mapValues(_ => ()))
    val identity = fattr.map({ case (id, v) => id -> Edge(id, id) }).partitionBy(attr.partitioner.get)
    output(o.identity, identity)
  }
}
