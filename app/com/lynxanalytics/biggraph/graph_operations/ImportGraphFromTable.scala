// Operations and other classes for importing data from tables.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ImportEdgeListForExistingVertexSetFromTableBase {
  class Input[T] extends MagicInputSignature {
    val rows = vertexSet
    val srcVidColumn = vertexAttribute[T](rows)
    val dstVidColumn = vertexAttribute[T](rows)
    val sources = vertexSet
    val destinations = vertexSet
    val srcVidAttr = vertexAttribute[T](sources)
    val dstVidAttr = vertexAttribute[T](destinations)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.sources.entity, inputs.destinations.entity)
    val embedding = edgeBundle(edges.idSet, inputs.rows.entity, EdgeBundleProperties.embedding)
  }

  def run[T: TypeTag](
    srcVidAttr: Attribute[T],
    dstVidAttr: Attribute[T],
    srcVidColumn: Attribute[T],
    dstVidColumn: Attribute[T])(implicit m: MetaGraphManager): Output[T] = {
    import Scripting._
    val op = {
      if (typeOf[T] =:= typeOf[String])
        new ImportEdgeListForExistingVertexSetFromTable()
      else if (typeOf[T] =:= typeOf[Long])
        new ImportEdgeListForExistingVertexSetFromTableLong()
      else assert(false,
        s"ImportEdgeListForExistingVertexSetFromTableBase is not supported for ${typeOf[T]}.")
    }.asInstanceOf[ImportEdgeListForExistingVertexSetFromTableBase[T]]
    op(
      op.srcVidColumn, srcVidColumn)(
        op.dstVidColumn, dstVidColumn)(
          op.srcVidAttr, srcVidAttr)(
            op.dstVidAttr, dstVidAttr).result
  }

  def runtimeSafe(
    srcVidAttr: Attribute[_],
    dstVidAttr: Attribute[_],
    srcVidColumn: Attribute[_],
    dstVidColumn: Attribute[_])(implicit m: MetaGraphManager): Output[_] = {
    val tt = srcVidAttr.typeTag
    runtimeSafeHelper(srcVidAttr, dstVidAttr, srcVidColumn, dstVidColumn)(tt, m)
  }

  private def runtimeSafeHelper[T: TypeTag](
    srcVidAttr: Attribute[_],
    dstVidAttr: Attribute[_],
    srcVidColumn: Attribute[_],
    dstVidColumn: Attribute[_])(implicit m: MetaGraphManager): Output[_] = {
    run(
      srcVidAttr.runtimeSafeCast[T],
      dstVidAttr.runtimeSafeCast[T],
      srcVidColumn.runtimeSafeCast[T],
      dstVidColumn.runtimeSafeCast[T])
  }

  def resolveEdges[T: reflect.ClassTag: Ordering](
    unresolvedEdges: UniqueSortedRDD[ID, (T, T)],
    srcVidAttr: AttributeData[T],
    dstVidAttr: AttributeData[T]): UniqueSortedRDD[ID, Edge] = {

    val partitioner = unresolvedEdges.partitioner.get

    val srcNameToVid = srcVidAttr.rdd
      .map { case (k, v) => v -> k }
      .assertUniqueKeys(partitioner)
    val dstNameToVid = {
      if (srcVidAttr.gUID == dstVidAttr.gUID)
        srcNameToVid
      else
        dstVidAttr.rdd
          .map { case (k, v) => v -> k }
          .assertUniqueKeys(partitioner)
    }
    val srcResolvedByDst = RDDUtils.hybridLookup(
      unresolvedEdges.map {
        case (edgeId, (srcName, dstName)) => srcName -> (edgeId, dstName)
      },
      srcNameToVid)
      .map { case (srcName, ((edgeId, dstName), srcVid)) => dstName -> (edgeId, srcVid) }

    RDDUtils.hybridLookup(srcResolvedByDst, dstNameToVid)
      .map { case (dstName, ((edgeId, srcVid), dstVid)) => edgeId -> Edge(srcVid, dstVid) }
      .sortUnique(partitioner)
  }
}
import ImportEdgeListForExistingVertexSetFromTableBase._
abstract class ImportEdgeListForExistingVertexSetFromTableBase[T: reflect.ClassTag: Ordering]
    extends TypedMetaGraphOp[Input[T], Output[T]] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input[T]()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
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

// Typed versions.
object ImportEdgeListForExistingVertexSetFromTable extends OpFromJson {
  def fromJson(j: JsValue) = new ImportEdgeListForExistingVertexSetFromTable()
}
case class ImportEdgeListForExistingVertexSetFromTable()
  extends ImportEdgeListForExistingVertexSetFromTableBase[String]

object ImportEdgeListForExistingVertexSetFromTableLong extends OpFromJson {
  def fromJson(j: JsValue) = new ImportEdgeListForExistingVertexSetFromTableLong()
}
case class ImportEdgeListForExistingVertexSetFromTableLong()
  extends ImportEdgeListForExistingVertexSetFromTableBase[Long]
