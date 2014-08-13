package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object VertexSetIntersection {
  class Input(numVertexSets: Int) extends MagicInputSignature {
    val vss = Range(0, numVertexSets).map {
      i => vertexSet(Symbol("vs" + i))
    }.toList
  }
  class Output(
      implicit instance: MetaGraphOperationInstance,
      input: Input) extends MagicOutput(instance) {

    val intersection = vertexSet
    // An embedding of the intersection into the first vertex set.
    val firstEmbedding = edgeBundle(
      intersection, input.vss(0).entity, EdgeBundleProperties.embedding)
  }
}
case class VertexSetIntersection(numVertexSets: Int)
    extends TypedMetaGraphOp[VertexSetIntersection.Input, VertexSetIntersection.Output] {

  import VertexSetIntersection._
  assert(numVertexSets >= 1)
  @transient override lazy val inputs = new Input(numVertexSets)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val intersection = inputs.vss.map(_.rdd)
      .reduce((rdd1, rdd2) => rdd1.sortedJoin(rdd2).mapValues(_ => ()))
    output(o.intersection, intersection)
    output(o.firstEmbedding, intersection.mapValuesWithKeys { case (id, _) => Edge(id, id) })
  }
}

object EdgeBundleIntersection {
  class Input(numEdgeBundles: Int) extends MagicInputSignature {
    val srcVS = vertexSet
    val dstVS = vertexSet
    val ebs = Range(0, numEdgeBundles).map {
      i => edgeBundle(srcVS, dstVS, name = Symbol("eb" + i))
    }.toList
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val intersection = edgeBundle(inputs.srcVS.entity, inputs.dstVS.entity)
  }
}
case class EdgeBundleIntersection(numEdgeBundles: Int)
    extends TypedMetaGraphOp[EdgeBundleIntersection.Input, EdgeBundleIntersection.Output] {
  import EdgeBundleIntersection._
  assert(numEdgeBundles >= 1)
  @transient override lazy val inputs = new Input(numEdgeBundles)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val intersection = inputs.ebs.map(_.rdd)
      .reduce((rdd1, rdd2) => rdd1.sortedJoin(rdd2).mapValues(_._1))
    output(o.intersection, intersection)
  }
}
