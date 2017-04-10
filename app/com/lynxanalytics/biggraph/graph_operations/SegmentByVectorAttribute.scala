// Creates a segmentation where each segment represents an element from a vector attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.SortedRDD

import SerializableType.Implicits._
import org.apache.spark
import scala.reflect.runtime.universe.TypeTag

object SegmentByVectorAttribute extends OpFromJson {
  class Output[T: SerializableType](implicit instance: MetaGraphOperationInstance,
                                    inputs: VertexAttributeInput[Vector[T]]) extends MagicOutput(instance) {
    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity, segments, EdgeBundleProperties.partialFunction)
    val label = vertexAttribute[T](segments)
  }
  def fromJson(j: JsValue) = SegmentByVectorAttribute()(SerializableType.fromJson(j \ "type"))
}
import SegmentByVectorAttribute._
case class SegmentByVectorAttribute[T: SerializableType]()
    extends TypedMetaGraphOp[VertexAttributeInput[Vector[T]], SegmentByVectorAttribute.Output[T]] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[Vector[T]]
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output[T]()(implicitly[SerializableType[T]], instance, inputs)
  override def toJson = Json.obj("type" -> implicitly[SerializableType[T]].toJson)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.meta.classTag
    implicit val paramCT = RuntimeSafeCastable.classTagFromTypeTag[T]
    implicit val runtimeContext = rc

    val linkBase =
      inputs.attr.rdd.flatMapValues(v => v).persist(spark.storage.StorageLevel.DISK_ONLY)
    val bucketing = Bucketing(linkBase, even = false)

    output(o.segments, bucketing.segments)
    output(o.label, bucketing.label)
    output(o.belongsTo, bucketing.belongsTo)
  }
}
