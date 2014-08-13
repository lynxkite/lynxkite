package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PulledOverVertexAttribute {
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
    val pop = PulledOverVertexAttribute[T]()
    pop(pop.originalAttr, originalAttr)(pop.injection, injection).result.pulledAttr
  }
}
case class PulledOverVertexAttribute[T]()
    extends TypedMetaGraphOp[PulledOverVertexAttribute.Input[T], PulledOverVertexAttribute.Output[T]] {
  import PulledOverVertexAttribute._
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

object PulledOverEdgeAttribute {
  class Input[T] extends MagicInputSignature {
    val originalSrc = vertexSet
    val originalDst = vertexSet
    val originalEB = edgeBundle(originalSrc, originalDst)
    val destinationSrc = vertexSet
    val destinationDst = vertexSet
    val destinationEB = edgeBundle(destinationSrc, destinationDst)
    // Ideally, we'd have an injection from destination to original here, but we
    // cannot yet(?) represent that for edge bundles. So we just assume destination is
    // embedded in original, which is the case if it is an induced bundle, the use case we
    // need now.
    val originalAttr = edgeAttribute[T](originalEB)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.originalAttr.typeTag
    val pulledAttr = edgeAttribute[T](inputs.destinationEB.entity)
  }
  def pullAttributeTo[T](
    originalAttr: EdgeAttribute[T], to: EdgeBundle)(
      implicit metaManager: MetaGraphManager): EdgeAttribute[T] = {
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    val pop = PulledOverEdgeAttribute[T]()
    pop(pop.originalAttr, originalAttr)(pop.destinationEB, to).result.pulledAttr
  }
}
case class PulledOverEdgeAttribute[T]()
    extends TypedMetaGraphOp[PulledOverEdgeAttribute.Input[T], PulledOverEdgeAttribute.Output[T]] {
  import PulledOverEdgeAttribute._
  @transient override lazy val inputs = new Input[T]()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val originalAttr = inputs.originalAttr.rdd
    val destinationEB = inputs.destinationEB.rdd
    val pulledAttr = originalAttr.sortedJoin(destinationEB)
      .mapValues { case (value, edge) => value }
    output(o.pulledAttr, pulledAttr)
  }

  override val isHeavy = true
}
