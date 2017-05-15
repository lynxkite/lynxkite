// Builds a graph from a set of edges provided as a vertex set and two string attributes (src/dst).
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.RDDUtils

import org.apache.spark

object VerticesToEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val srcAttr = vertexAttribute[String](vs)
    val dstAttr = vertexAttribute[String](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val (vs, es) = graph
    val stringId = vertexAttribute[String](vs)
    val embedding = edgeBundle(es.idSet, inputs.vs.entity, EdgeBundleProperties.embedding)
  }
  def fromJson(j: JsValue) = VerticesToEdges()
}
import VerticesToEdges._
case class VerticesToEdges() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val partitioner = inputs.vs.rdd.partitioner.get
    val srcAttr = inputs.srcAttr.rdd
    val dstAttr = inputs.dstAttr.rdd
    val names = (srcAttr.values ++ dstAttr.values).distinct
    val idToName = names
      .randomNumbered(partitioner.numPartitions)
      .persist(spark.storage.StorageLevel.DISK_ONLY)
    val nameToId = idToName
      .map(_.swap)
      .sortUnique(partitioner)
      .persist(spark.storage.StorageLevel.DISK_ONLY)
    val edgeSrcDst = srcAttr.sortedJoin(dstAttr)
    val bySrc = edgeSrcDst.map {
      case (edgeId, (src, dst)) => src -> (edgeId, dst)
    }
    val byDst = HybridRDD(bySrc, partitioner, even = true).lookupAndRepartition(nameToId).map {
      case (src, ((edgeId, dst), sid)) => dst -> (edgeId, sid)
    }
    val edges = HybridRDD(byDst, partitioner, even = true).lookup(nameToId).map {
      case (dst, ((edgeId, sid), did)) => edgeId -> Edge(sid, did)
    }.sortUnique(partitioner)
    val embedding = inputs.vs.rdd.mapValuesWithKeys { case (id, _) => Edge(id, id) }
    val idToNameForOutput = RDDUtils.maybeRepartitionForOutput(idToName)
    output(o.vs, idToNameForOutput.mapValues(_ => ()))
    output(o.es, edges)
    output(o.stringId, idToNameForOutput)
    output(o.embedding, embedding)
  }
}
