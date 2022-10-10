// Takes a "role" attribute that has either "test" or "train" set as value.
// Based on the role the operation partitions the "attr" input into
// two separate vertexAttributes. The new vertexAttributes will be undefined
// on those vertices where their respective role is not set.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object PartitionAttribute extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vs = vertexSet
    val attr = vertexAttribute[T](vs)
    val role = vertexAttribute[String](vs)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.attr.typeTag
    val test = vertexAttribute[T](inputs.vs.entity)
    val train = vertexAttribute[T](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = PartitionAttribute()
}
import PartitionAttribute._
case class PartitionAttribute[T]() extends SparkOperation[Input[T], Output[T]] {
  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output[T],
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val attr = inputs.attr.rdd
    val role = inputs.role.rdd
    val attrWithRole = attr.sortedJoin(role)
    output(o.test, attrWithRole.filter(_._2._2 == "test").mapValues(_._1))
    output(o.train, attrWithRole.filter(_._2._2 == "train").mapValues(_._1))
  }
}
