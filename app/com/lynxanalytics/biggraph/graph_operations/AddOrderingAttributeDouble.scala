package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object AddOrderingAttributeDouble extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val sortKey = vertexAttribute[Double](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val ordinal = vertexAttribute[Long](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = AddOrderingAttributeDouble((j \ "ascending").as[Boolean])
}
import AddOrderingAttributeDouble._
case class AddOrderingAttributeDouble(ascending: Boolean) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()
  override def toJson = Json.obj("ascending" -> ascending)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val sortKey = inputs.sortKey.rdd

    val swapped = sortKey.map({
      case (id, value) => (value, id)
    })

    val sorted = swapped.sortByKey(ascending)
    val zipped = sorted.zipWithIndex()
    val result = zipped.map({
      case ((key, id), idx) => id -> idx
    })
    output(o.ordinal, result.toSortedRDD(sortKey.partitioner.get))
  }
}

