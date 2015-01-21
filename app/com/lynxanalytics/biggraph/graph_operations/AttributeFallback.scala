package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.SortedSet
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

/*
 * This operation fills the missing values of an attribute from another attribute
 */
object AttributeFallback extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vs = vertexSet
    val originalAttr = vertexAttribute[T](vs)
    val defaultAttr = vertexAttribute[T](vs)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance, inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.originalAttr.typeTag
    val defaultedAttr = vertexAttribute[T](inputs.vs.entity)
  }
  def fromJson(j: play.api.libs.json.JsValue) = AttributeFallback[Any]()
}
import AttributeFallback._
case class AttributeFallback[T]() extends TypedMetaGraphOp[Input[T], Output[T]] {
  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(
      o.defaultedAttr,
      inputs.originalAttr.rdd.fullOuterJoin(inputs.defaultAttr.rdd)
        .mapValues { case (origOpt, defaultOpt) => origOpt.getOrElse(defaultOpt.get) })
  }
}
