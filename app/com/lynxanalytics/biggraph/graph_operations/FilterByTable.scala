// Operation to filter a vertex set to a list of IDs from the table.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object FilterByTable extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val t = table
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val vs = vertexSet
    val identity = edgeBundle(vs, inputs.vs.entity, EdgeBundleProperties.embedding)
  }
  def fromJson(j: JsValue) = FilterByTable((j \ "column").as[String])
}
import FilterByTable._
case class FilterByTable(column: String) extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("column" -> column)
  def execute(
    inputDatas: DataSet,
    o: Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = inputs.vs.rdd.partitioner.get
    val ids = inputs.t.data.df.select(column).rdd.map(_.getLong(0))
    val vs = ids.map((_, ())).sortUnique(partitioner).sortedJoin(inputs.vs.rdd).mapValues(v => ())
    output(o.vs, vs)
    output(o.identity, vs.mapValuesWithKeys { case (k, _) => Edge(k, k) })
  }
}
