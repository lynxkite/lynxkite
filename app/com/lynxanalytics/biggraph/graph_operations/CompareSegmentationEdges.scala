package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object CompareSegmentationEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    // Inputs for making sure that the two edge bundle inputs are based on compatible
    // vertex sets. These are not used directly.
    val parentVs = vertexSet
    val goldenVs = vertexSet
    val testVs = vertexSet
    val goldenBelongsTo = edgeBundle(
      parentVs, goldenVs, EdgeBundleProperties.identity)
    val testBelongsTo = edgeBundle(
      parentVs, testVs, EdgeBundleProperties.identity)
    // The actually used inputs.
    val goldenEdges = edgeBundle(goldenVs, goldenVs)
    val testEdges = edgeBundle(testVs, testVs)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val precision = scalar[Double]
    val recall = scalar[Double]
    val presentInGolden = edgeAttribute[Double](inputs.testEdges.entity)
    val presentInTest = edgeAttribute[Double](inputs.goldenEdges.entity)
  }
  def fromJson(j: JsValue) = CompareSegmentationEdges()
}
import CompareSegmentationEdges._
case class CompareSegmentationEdges()
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val golden = inputs.goldenEdges.rdd
    val test = inputs.testEdges.rdd
    val partitioner = golden.partitioner.get

    val goldenKeys = golden
      .map { case (id, edge) => edge -> id }
      .sort(partitioner)
      .distinctByKey()
    val testKeys = test
      .map { case (id, edge) => edge -> id }
      .sort(partitioner)
      .distinctByKey()
    val goldenCount = goldenKeys.count
    val testCount = testKeys.count
    val intersectionKeys = goldenKeys.sortedJoin(testKeys)
    val intersectionCount = intersectionKeys.count

    output(o.recall, 1.0 * intersectionCount / goldenCount)
    output(o.precision, 1.0 * intersectionCount / testCount)
    output(
      o.presentInTest,
      intersectionKeys
        .map { case (_, (goldenId, _)) => (goldenId, 1.0) }
        .sortUnique(golden.partitioner.get))
    output(
      o.presentInGolden,
      intersectionKeys
        .map { case (_, (_, testId)) => (testId, 1.0) }
        .sortUnique(test.partitioner.get))

  }
}
