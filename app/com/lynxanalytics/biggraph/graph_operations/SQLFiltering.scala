// Operations for the "Filter with SQL" frontend operation.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object IDTableToVertexSet extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
  }
  def fromJson(j: JsValue) = IDTableToVertexSet((j \ "column").as[String])
}
case class IDTableToVertexSet(column: String)
  extends SparkOperation[IDTableToVertexSet.Input, IDTableToVertexSet.Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new IDTableToVertexSet.Input
  def outputMeta(instance: MetaGraphOperationInstance) = new IDTableToVertexSet.Output()(instance)
  override def toJson = Json.obj("column" -> column)
  def execute(
    inputDatas: DataSet,
    o: IDTableToVertexSet.Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = rc.partitionerForNRows(inputs.t.data.df.count)
    val vs = inputs.t.data.df.select(column).rdd.map(row => (row.getLong(0), ()))
    output(o.vs, vs.sortUnique(partitioner))
  }
}

object EmbeddingByID extends OpFromJson {
  class Input extends MagicInputSignature {
    val original = vertexSet
    val filtered = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val identity = edgeBundle(inputs.filtered.entity, inputs.original.entity, EdgeBundleProperties.embedding)
  }
  def fromJson(j: JsValue) = EmbeddingByID()
}
case class EmbeddingByID()
  extends SparkOperation[EmbeddingByID.Input, EmbeddingByID.Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new EmbeddingByID.Input
  def outputMeta(instance: MetaGraphOperationInstance) = new EmbeddingByID.Output()(instance, inputs)
  override def toJson = Json.obj()
  def execute(
    inputDatas: DataSet,
    o: EmbeddingByID.Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.identity, inputs.filtered.rdd.mapValuesWithKeys { case (k, _) => Edge(k, k) })
  }
}
