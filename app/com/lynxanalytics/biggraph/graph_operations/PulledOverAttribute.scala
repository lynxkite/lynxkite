package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PulledOverAttribute {
  class Input[T] extends MagicInputSignature {
    val originalVS = vertexSet
    val destinationVS = vertexSet
    val injection = edgeBundle(destinationVS, originalVS, EdgeBundleProperties.injection)
    val originalAttr = vertexAttribute[T](originalVS)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.originalAttr.typeTag
    val pulledAttr = vertexAttribute[T](inputs.destinationVS.entity)
  }
  def pullAttributeVia[T](
    originalAttr: VertexAttribute[T], injection: EdgeBundle)(
      implicit metaManager: MetaGraphManager): VertexAttribute[T] = {
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    val pop = PulledOverAttribute[T]()
    pop(pop.originalAttr, originalAttr)(pop.injection, injection).result.pulledAttr
  }
}
import PulledOverAttribute._
case class PulledOverAttribute[T]()
    extends TypedMetaGraphOp[Input[T], Output[T]] {
  @transient override lazy val inputs = new Input[T]()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val injectionEntity = inputs.injection.meta
    val injection = inputs.injection.rdd
    val originalAttr = inputs.originalAttr.rdd
    val pulledAttr =
      if (injectionEntity.properties.isIdentity) {
        originalAttr.sortedJoin(injection).mapValues { case (value, edge) => value }
      } else {
        val destinationVS = inputs.destinationVS.rdd
        val originalToDestinationID = injection
          .map { case (id, edge) => (edge.dst, edge.src) }
          .toSortedRDD(destinationVS.partitioner.get)
        implicit val ct = inputs.originalAttr.meta.classTag
        originalAttr.sortedJoin(originalToDestinationID)
          .map { case (originalID, (value, destinationID)) => (destinationID, value) }
          .toSortedRDD(destinationVS.partitioner.get)
      }
    output(o.pulledAttr, pulledAttr)
  }

  override val isHeavy = true
}
