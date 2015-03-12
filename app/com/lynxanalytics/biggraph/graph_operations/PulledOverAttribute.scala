package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PulledOverVertexAttribute extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val originalVS = vertexSet
    val destinationVS = vertexSet
    val function = edgeBundle(destinationVS, originalVS, EdgeBundleProperties.partialFunction)
    val originalAttr = vertexAttribute[T](originalVS)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.originalAttr.typeTag
    val pulledAttr = vertexAttribute[T](inputs.destinationVS.entity)
  }
  def pullAttributeVia[T](
    originalAttr: Attribute[T], function: EdgeBundle)(
      implicit metaManager: MetaGraphManager): Attribute[T] = {
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    val pop = PulledOverVertexAttribute[T]()
    pop(pop.originalAttr, originalAttr)(pop.function, function).result.pulledAttr
  }
  def fromJson(j: JsValue) = PulledOverVertexAttribute()
}
case class PulledOverVertexAttribute[T]()
    extends TypedMetaGraphOp[PulledOverVertexAttribute.Input[T], PulledOverVertexAttribute.Output[T]] {
  import PulledOverVertexAttribute._
  override val isHeavy = true
  @transient override lazy val inputs = new Input[T]()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val functionEntity = inputs.function.meta
    val function = inputs.function.rdd
    val originalAttr = inputs.originalAttr.rdd
    val originalPartitioner = originalAttr.partitioner.get
    val pulledAttr =
      if (functionEntity.properties.isIdPreserving) {
        val joinableFunction = function.sortedRepartition(originalPartitioner)
        originalAttr.sortedJoin(joinableFunction).mapValues { case (value, edge) => value }
      } else {
        val destinationVS = inputs.destinationVS.rdd
        val originalToDestinationID = function
          .map { case (id, edge) => (edge.dst, edge.src) }
          .toSortedRDD(originalPartitioner)
        implicit val ct = inputs.originalAttr.meta.classTag
        originalToDestinationID.sortedJoin(originalAttr)
          .values
          .toSortedRDD(destinationVS.partitioner.get)
      }
    output(o.pulledAttr, pulledAttr)
  }
}
