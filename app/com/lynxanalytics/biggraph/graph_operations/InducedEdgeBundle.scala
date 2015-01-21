package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.Partitioner

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object InducedEdgeBundle extends OpFromJson {
  class Input(induceSrc: Boolean, induceDst: Boolean) extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val srcImage = if (induceSrc) vertexSet else null
    val dstImage = if (induceDst) vertexSet else null
    val srcMapping =
      if (induceSrc) edgeBundle(src, srcImage, EdgeBundleProperties.partialFunction) else null
    val dstMapping =
      if (induceDst) edgeBundle(dst, dstImage, EdgeBundleProperties.partialFunction) else null
    val edges = edgeBundle(src, dst)
  }
  class Output(induceSrc: Boolean, induceDst: Boolean)(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val induced = {
      val src = if (induceSrc) inputs.srcImage else inputs.src
      val dst = if (induceDst) inputs.dstImage else inputs.dst
      edgeBundle(src.entity, dst.entity)
    }
    val embedding = edgeBundle(
      induced.asVertexSet, inputs.edges.asVertexSet, EdgeBundleProperties.embedding)
  }
  def fromJson(j: play.api.libs.json.JsValue) = InducedEdgeBundle((j \ "induceSrc").as[Boolean], (j \ "induceDst").as[Boolean])
}
import InducedEdgeBundle._
case class InducedEdgeBundle(induceSrc: Boolean = true, induceDst: Boolean = true)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input(induceSrc, induceDst)

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output(induceSrc, induceDst)(instance, inputs)
  override def toJson = play.api.libs.json.Json.obj("induceSrc" -> induceSrc, "induceDst" -> induceDst)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val instance = output.instance
    val src = inputs.src.rdd
    val dst = inputs.dst.rdd
    val edges = inputs.edges.rdd

    def getMapping(mappingInput: MagicInputSignature#EdgeBundleTemplate,
                   partitioner: Partitioner): SortedRDD[ID, ID] = {
      val mappingEntity = mappingInput.entity
      val mappingEdges = mappingInput.rdd
      if (mappingEntity.properties.isIdentity) {
        // We save a shuffle in this case.
        mappingEdges.mapValuesWithKeys { case (id, _) => id }
      } else {
        mappingEdges
          .map { case (id, edge) => (edge.src, edge.dst) }
          .toSortedRDD(partitioner)
      }
    }

    val srcInduced = if (!induceSrc) edges else {
      val srcPartitioner = src.partitioner.get
      val bySrc = edges
        .map { case (id, edge) => (edge.src, (id, edge)) }
        .toSortedRDD(srcPartitioner)
        .sortedJoin(getMapping(inputs.srcMapping, srcPartitioner))
        .mapValues { case ((id, edge), newSrc) => (id, Edge(newSrc, edge.dst)) }
      bySrc.values
    }
    val dstInduced = if (!induceDst) srcInduced else {
      val dstPartitioner = dst.partitioner.get
      val byDst = srcInduced
        .map { case (id, edge) => (edge.dst, (id, edge)) }
        .toSortedRDD(dstPartitioner)
        .sortedJoin(getMapping(inputs.dstMapping, dstPartitioner))
        .mapValues { case ((id, edge), newDst) => (id, Edge(edge.src, newDst)) }
      byDst.values
    }
    val induced = dstInduced.toSortedRDD(edges.partitioner.get)
    output(o.induced, induced)
    output(o.embedding, induced.mapValuesWithKeys { case (id, _) => Edge(id, id) })
  }
}
