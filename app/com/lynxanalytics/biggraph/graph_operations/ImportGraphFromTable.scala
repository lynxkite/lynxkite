// Operations and other classes for importing data from tables.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ImportEdgeListForExistingVertexSetFromTable extends OpFromJson {
  class Input extends MagicInputSignature {
    val rows = vertexSet
    val srcVidColumn = vertexAttribute[String](rows)
    val dstVidColumn = vertexAttribute[String](rows)
    val sources = vertexSet
    val destinations = vertexSet
    val srcVidAttr = vertexAttribute[String](sources)
    val dstVidAttr = vertexAttribute[String](destinations)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.sources.entity, inputs.destinations.entity)
    val embedding = edgeBundle(edges.idSet, inputs.rows.entity, EdgeBundleProperties.embedding)
  }
  def fromJson(j: JsValue) =
    ImportEdgeListForExistingVertexSetFromTable()

  def resolveEdges(
    unresolvedEdges: UniqueSortedRDD[ID, (String, String)],
    srcVidAttr: AttributeData[String],
    dstVidAttr: AttributeData[String]): UniqueSortedRDD[ID, Edge] = {

    val partitioner = unresolvedEdges.partitioner.get

    val srcStringToVid =
      ImportCommon.checkIdMapping(srcVidAttr.rdd.map { case (k, v) => v -> k }, partitioner)
    val dstStringToVid = {
      if (srcVidAttr.gUID == dstVidAttr.gUID)
        srcStringToVid
      else
        ImportCommon.checkIdMapping(
          dstVidAttr.rdd.map { case (k, v) => v -> k }, partitioner)
    }
    val srcResolvedByDst = RDDUtils.hybridLookup(
      unresolvedEdges.map {
        case (edgeId, (srcString, dstString)) => srcString -> (edgeId, dstString)
      },
      srcStringToVid)
      .map { case (srcString, ((edgeId, dstString), srcVid)) => dstString -> (edgeId, srcVid) }

    RDDUtils.hybridLookup(srcResolvedByDst, dstStringToVid)
      .map { case (dstString, ((edgeId, srcVid), dstVid)) => edgeId -> Edge(srcVid, dstVid) }
      .sortUnique(partitioner)
  }
}
case class ImportEdgeListForExistingVertexSetFromTable()
    extends TypedMetaGraphOp[ImportEdgeListForExistingVertexSetFromTable.Input, ImportEdgeListForExistingVertexSetFromTable.Output] {
  import ImportEdgeListForExistingVertexSetFromTable._
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    // Join the source and destination columns of the table to import.
    // If there were null values in the original DataFrame, then those
    // will end up as missing keys in srcVidColumn and dstVidColumns,
    // and this join will just discard any rows having those.
    val unresolvedEdges = inputs.srcVidColumn.rdd
      .sortedJoin(inputs.dstVidColumn.rdd)

    val edges = resolveEdges(
      unresolvedEdges, inputs.srcVidAttr.data, inputs.dstVidAttr.data)

    val embedding = edges.mapValuesWithKeys { case (id, _) => Edge(id, id) }

    output(o.edges, edges)
    output(o.embedding, embedding)
  }
}

object ImportAttributesForExistingVertexSetFromTable extends OpFromJson {
  class Input extends MagicInputSignature {
    val tableVs = vertexSet
    val idColumn = vertexAttribute[String](tableVs)
    val graphVs = vertexSet
    val idAttr = vertexAttribute[String](graphVs)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val pullFunction = edgeBundle(
      inputs.graphVs.entity,
      inputs.tableVs.entity,
      EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) =
    ImportAttributesForExistingVertexSetFromTable()
}
case class ImportAttributesForExistingVertexSetFromTable()
    extends TypedMetaGraphOp[ImportAttributesForExistingVertexSetFromTable.Input, ImportAttributesForExistingVertexSetFromTable.Output] {
  import ImportAttributesForExistingVertexSetFromTable._

  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = inputs.tableVs.rdd.partitioner.get
    // We assumes that the ID column in the table and the ID attribute of the
    // graph both have unique values. If not, then for each ID with multiplicity,
    // sortedJoin will keep pairing rows with vertices until one of the runs out
    // first.
    val tableStringToId = inputs.idColumn.rdd
      .map { case (id, string) => (string, id) }
      .sortUnique(partitioner)
    val graphStringToId = inputs.idAttr.rdd
      .map { case (id, string) => (string, id) }
      .sortUnique(partitioner)
    val mapping = tableStringToId
      .sortedJoin(graphStringToId)
      .values
      .map { case (tableVid, graphVid) => Edge(graphVid, tableVid) }
      .randomNumbered(partitioner)

    output(o.pullFunction, mapping)
  }
}
