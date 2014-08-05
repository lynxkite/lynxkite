package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object WeightedOutDegree {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: EdgeAttributeInput[Double])
      extends MagicOutput(instance) {
    val outDegree = vertexAttribute[Double](inputs.src.entity)
  }
}
import WeightedOutDegree._
case class WeightedOutDegree() extends TypedMetaGraphOp[EdgeAttributeInput[Double], Output] {
  @transient override lazy val inputs = new EdgeAttributeInput[Double]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vsA = inputs.src.rdd
    val weights = inputs.attr.rdd
    val outdegrees = inputs.es.rdd.sortedJoin(weights)
      .map { case (_, (edge, weight)) => edge.src -> weight }
      .reduceByKey(vsA.partitioner.get, _ + _).toSortedRDD
    // TODO: update after reduceBySortedKey is implemented, https://github.com/biggraph/biggraph/issues/333
    val result = vsA.sortedLeftOuterJoin(outdegrees).mapValues(_._2.getOrElse(0.0))
    output(o.outDegree, result)
  }
}
