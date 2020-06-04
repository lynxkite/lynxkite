// Outputs a ranking attribute that starts at 0 for the largest/smallest value.
// Input: the vertex attribute holding the values on which the ranking is based.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import scala.reflect.runtime.universe.TypeTag

object AddRankingAttribute extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vertices = vertexSet
    val sortKey = vertexAttribute[T](vertices)
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input[_]) extends MagicOutput(instance) {
    val ordinal = vertexAttribute[Double](inputs.vertices.entity)
  }

  def run[T](
    attr: Attribute[T], ascending: Boolean)(implicit m: MetaGraphManager): Attribute[Double] = {
    import Scripting._
    val st = SerializableType[T](attr.typeTag)
    val op = AddRankingAttribute[T](ascending)(st)
    op(op.sortKey, attr).result.ordinal
  }

  // Need to specify the return type, otherwise the compiler doesn't figure it out properly
  // and generates faulty Java bytecode (keeping the abstract method from FromJson). This
  // results in a runtime error when trying to call the abstract fromJson().
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    val ascending = (j \ "ascending").as[Boolean]
    val tpe = SerializableType.fromJson(j \ "type")
    AddRankingAttribute(ascending)(tpe)
  }
}
import AddRankingAttribute._
case class AddRankingAttribute[T: SerializableType](
    ascending: Boolean) extends SparkOperation[Input[T], Output] {
  @transient override lazy val inputs = new Input[T]()
  override def toJson = Json.obj(
    "ascending" -> ascending,
    "type" -> implicitly[SerializableType[T]].toJson)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
    inputDatas: DataSet,
    o: Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    import SerializableType.Implicits._
    implicit val ds = inputDatas
    val sortKey = inputs.sortKey.rdd
    val swapped = sortKey.map(_.swap)
    val ord = implicitly[Ordering[T]]
    val sorted = swapped.sortByKey(ascending)
    val zipped = sorted.zipWithIndex()
    val result = zipped.map { case ((key, id), idx) => id -> idx.toDouble }
    output(o.ordinal, result.sortUnique(sortKey.partitioner.get))
  }
}
