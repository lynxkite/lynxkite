// Outputs a new vertex attribute that is ordered with respect to a vertexAttribute[Double]
// Input: the vertex attribute holding the values on which the sorting is based.
// Parameter ascending defines the sort order.
// TODO: Remove the limitation on the input vertex attribute type (i.e., it is double
// at the moment, but it should also work with Long, String etc.)

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object AddRankingAttributeString extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val sortKey = vertexAttribute[String](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val ordinal = vertexAttribute[Long](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = AddRankingAttributeString((j \ "ascending").as[Boolean])
}
import AddRankingAttributeString._
case class AddRankingAttributeString(ascending: Boolean) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()
  override def toJson = Json.obj("ascending" -> ascending)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val sortKey = inputs.sortKey.rdd

    val swapped = sortKey.map(_.swap)

    val sorted = swapped.sortByKey(ascending)
    val zipped = sorted.zipWithIndex()
    val result = zipped.map { case ((key, id), idx) => id -> idx }
    output(o.ordinal, result.sortUnique(sortKey.partitioner.get))
  }
}
