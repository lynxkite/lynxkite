// Takes two segmentations ("golden" and "test") from the same base project as input.
// They should have the same set of vertices as their common base project. (There should
// be a one-to-one correspondence between the vertex sets.)
//
// Computes the difference between the edges of "golden" and the edges of "test" and computes
// precision and recall of the "test" edges in relation to the "golden" edges. Parallel edges
// are counted as one.
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
      parentVs,
      goldenVs,
      EdgeBundleProperties.identity)
    val testBelongsTo = edgeBundle(
      parentVs,
      testVs,
      EdgeBundleProperties.identity)
    // The actually used inputs.
    val goldenEdges = edgeBundle(goldenVs, goldenVs)
    val testEdges = edgeBundle(testVs, testVs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val precision = scalar[Double]
    val recall = scalar[Double]
    // An attribute on the test edges. It is assigned 1.0 if there is a corresponding
    // golden edge, otherwise not defined.
    val presentInGolden = edgeAttribute[Double](inputs.testEdges.entity)
    // An attribute on the golden edges. It is assigned 1.0 if there is a corresponding
    // test edge, otherwise not defined.
    val presentInTest = edgeAttribute[Double](inputs.goldenEdges.entity)
  }
  def fromJson(j: JsValue) = CompareSegmentationEdges()
}
import CompareSegmentationEdges._
case class CompareSegmentationEdges()
    extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val golden = inputs.goldenEdges.rdd
    val test = inputs.testEdges.rdd
    val partitioner = golden.partitioner.get

    val goldenKeys = golden
      .map(_.swap)
      .sort(partitioner)
      .distinctByKey()
    val testKeys = test
      .map(_.swap)
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
