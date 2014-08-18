package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object VertexSetUnion {
  class Input(numVertexSets: Int) extends MagicInputSignature {
    val vss = Range(0, numVertexSets).map {
      i => vertexSet(Symbol("vs" + i))
    }.toList
  }
  class Output(numVertexSets: Int)(
      implicit instance: MetaGraphOperationInstance,
      input: Input) extends MagicOutput(instance) {

    val union = vertexSet
    // Injections of the original vertex sets into the union.
    val injections = Range(0, numVertexSets)
      .map(i => edgeBundle(
        input.vss(i).entity, union, EdgeBundleProperties.injection, name = Symbol("injection" + i)))
  }
}
case class VertexSetUnion(numVertexSets: Int)
    extends TypedMetaGraphOp[VertexSetUnion.Input, VertexSetUnion.Output] {

  import VertexSetUnion._
  assert(numVertexSets >= 1)
  @transient override lazy val inputs = new Input(numVertexSets)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output(numVertexSets)(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val unionWithOldIds = rc.sparkContext.union(
      inputs.vss
        .map(_.rdd)
        .zipWithIndex
        .map { case (rdd, idx) => rdd.mapValues(_ => idx) }).randomNumbered(rc.defaultPartitioner)
    output(o.union, unionWithOldIds.mapValues(_ => ()))
    for (idx <- 0 until numVertexSets) {
      output(
        o.injections(idx),
        unionWithOldIds
          .filter { case (newId, (oldId, sourceIdx)) => sourceIdx == idx }
          .map { case (newId, (oldId, sourceIdx)) => Edge(oldId, newId) }
          .randomNumbered(rc.defaultPartitioner))
    }
  }
}
