// Operations for taking the union of vertex sets and edge bundles.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.HashPartitioner
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object VertexSetUnion extends OpFromJson {
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
  def fromJson(j: JsValue) = VertexSetUnion((j \ "numVertexSets").as[Int])
}
case class VertexSetUnion(numVertexSets: Int)
    extends TypedMetaGraphOp[VertexSetUnion.Input, VertexSetUnion.Output] {
  import VertexSetUnion._

  override val isHeavy = true

  assert(numVertexSets >= 1, s"Cannot take the union of $numVertexSets vertex sets.")
  @transient override lazy val inputs = new Input(numVertexSets)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output(numVertexSets)(instance, inputs)
  override def toJson = Json.obj("numVertexSets" -> numVertexSets)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vss = inputs.vss.map(_.rdd)

    val totalNumPartitions = vss.map(_.partitioner.get.numPartitions).sum
    val unionPartitioner = new HashPartitioner(totalNumPartitions)
    val unionWithOldIds = rc.sparkContext.union(
      vss
        .zipWithIndex
        .map { case (rdd, idx) => rdd.mapValues(_ => idx) }).randomNumbered(unionPartitioner)
    output(o.union, unionWithOldIds.mapValues(_ => ()))
    for (idx <- 0 until numVertexSets) {
      output(
        o.injections(idx),
        unionWithOldIds
          .filter { case (newId, (oldId, sourceIdx)) => sourceIdx == idx }
          .map { case (newId, (oldId, sourceIdx)) => Edge(oldId, newId) }
          .randomNumbered(vss(idx).partitioner.get))
    }
  }
}

object EdgeBundleUnion extends OpFromJson {
  class Input(numEdgeBundles: Int) extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val idSets = Range(0, numEdgeBundles).map {
      i => vertexSet(Symbol("id_set" + i))
    }.toList
    val idSetUnion = vertexSet
    val injections = Range(0, numEdgeBundles).map {
      i =>
        edgeBundle(
          idSets(i),
          idSetUnion,
          requiredProperties = EdgeBundleProperties.injection,
          name = Symbol("injection" + i))
    }.toList
    val ebs = Range(0, numEdgeBundles).map {
      i => edgeBundle(src, dst, idSet = idSets(i), name = Symbol("eb" + i))
    }.toList
  }
  class Output(numEdgeBundles: Int)(
      implicit instance: MetaGraphOperationInstance,
      input: Input) extends MagicOutput(instance) {
    val union = edgeBundle(input.src.entity, input.dst.entity, idSet = input.idSetUnion.entity)
  }
  def fromJson(j: JsValue) = EdgeBundleUnion((j \ "numEdgeBundles").as[Int])
}
case class EdgeBundleUnion(numEdgeBundles: Int)
    extends TypedMetaGraphOp[EdgeBundleUnion.Input, EdgeBundleUnion.Output] {
  import EdgeBundleUnion._

  override val isHeavy = true

  assert(numEdgeBundles >= 1, s"Cannot take the union of $numEdgeBundles edge bundles.")
  @transient override lazy val inputs = new Input(numEdgeBundles)

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output(numEdgeBundles)(instance, inputs)
  override def toJson = Json.obj("numEdgeBundles" -> numEdgeBundles)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val idSetUnion = inputs.idSetUnion.rdd
    val reIdedEbs = Range(0, numEdgeBundles).map { i =>
      val eb = inputs.ebs(i).rdd
      // Here typically none of the injections are identity, so we don't special case for that.
      val injection =
        inputs.injections(i).rdd
          .map { case (_, e) => e.src -> e.dst }
          .sortUnique(eb.partitioner.get)
      eb.sortedJoin(injection).map { case (oldId, (edge, newId)) => (newId, edge) }
    }
    output(o.union, rc.sparkContext.union(reIdedEbs).sortUnique(idSetUnion.partitioner.get))
  }
}
