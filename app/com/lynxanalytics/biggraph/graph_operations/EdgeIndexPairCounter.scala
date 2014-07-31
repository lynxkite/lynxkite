package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EdgeIndexPairCounter {
  class Input extends MagicInputSignature {
    val srcVS = vertexSet
    val dstVS = vertexSet
    val eb = edgeBundle(srcVS, dstVS)
    val xIndices = edgeAttribute[Int](eb)
    val yIndices = edgeAttribute[Int](eb)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val counts = scalar[Map[(Int, Int), Int]]
  }
}
import EdgeIndexPairCounter._
case class EdgeIndexPairCounter() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val xIndices = inputs.xIndices.rdd
    val yIndices = inputs.yIndices.rdd
    output(o.counts, xIndices.sortedJoin(yIndices)
      .aggregate(mutable.Map[(Int, Int), Int]().withDefaultValue(0))(
        {
          case (map, (id, pair)) =>
            map(pair) += 1
            map
        },
        {
          case (map1, map2) =>
            map2.foreach { case (k, v) => map1(k) += v }
            map1
        }).toMap)
  }
}
