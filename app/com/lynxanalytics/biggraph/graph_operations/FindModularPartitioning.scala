package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object FindModularPartitioning extends OpFromJson {
  class Input() extends MagicInputSignature {
    val vs = vertexSet
    val edgeIds = vertexSet
    val edges = edgeBundle(vs, vs, idSet = edgeIds)
    val weights = vertexAttribute[Double](edgeIds)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val partitions = vertexSet
    val containedIn = edgeBundle(
      inputs.vs.entity, partitions, properties = EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) = FindModularPartitioning()
}
import FindModularPartitioning._
case class FindModularPartitioning() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vs = inputs.vs.rdd
    val vPart = vs.partitioner.get
    var members: SortedRDD[ID, ID] = vs.mapValuesWithKeys(_._1)
    var weightsFromFinals: SortedRDD[ID, (Double, Double)] =
      rc.sparkContext.emptyRDD[(ID, (Double, Double))].toSortedRDD(vPart)
    var connections: SortedRDD[(ID, ID), Double] = inputs.edges.rdd.sortedJoin(inputs.weights.rdd)
      .map { case (id, (e, w)) => ((e.src, e.dst), w) }
      .reduceBySortedKey(vPart, _ + _)
    while (connections.count() > 0) {
    }
    val partitions = members.groupByKey.randomNumbered(vPart).mapValues(_._2)
    output(o.partitions, partitions.mapValues(_ => ()))
    output(
      o.containedIn,
      partitions
        .flatMap { case (p, members) => members.map(m => Edge(m, p)) }
        .randomNumbered(vPart))
  }
}
