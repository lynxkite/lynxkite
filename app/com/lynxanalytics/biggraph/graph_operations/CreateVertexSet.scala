// Creates a new vertex set with a given size.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object CreateVertexSet extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
    val ordinal = vertexAttribute[Double](vs)
  }
  def fromJson(j: JsValue) = CreateVertexSet((j \ "size").as[Long])
}
import CreateVertexSet._
case class CreateVertexSet(size: Long) extends SparkOperation[NoInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new NoInput

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj("size" -> size)

  def execute(
    inputDatas: DataSet,
    o: Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    // NumericRanges are special-cased in parallelize so that only the range bounds are transmitted
    // for each partition.
    // https://github.com/apache/spark/blob/v1.3.0/core/src/main/scala/org/apache/spark/rdd/ParallelCollectionRDD.scala#L142
    val partitioner = rc.partitionerForNRows(size)
    val ordinals = rc.sparkContext.parallelize(0L until size, partitioner.numPartitions).map(_.toDouble)
    val attr = ordinals.randomNumbered(partitioner)
    output(o.vs, attr.mapValues(_ => ()))
    output(o.ordinal, attr)
  }
}
