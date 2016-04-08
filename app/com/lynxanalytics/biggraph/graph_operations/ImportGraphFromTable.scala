// Operations and other classes for importing data from tables.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ImportEdgeListForExistingVertexSetFromTableBase {
  class Input[A, B] extends MagicInputSignature {
    val rows = vertexSet
    val srcVidColumn = vertexAttribute[A](rows)
    val dstVidColumn = vertexAttribute[B](rows)
    val sources = vertexSet
    val destinations = vertexSet
    val srcVidAttr = vertexAttribute[A](sources)
    val dstVidAttr = vertexAttribute[B](destinations)
  }
  class Output[A, B](implicit instance: MetaGraphOperationInstance,
                     inputs: Input[A, B])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.sources.entity, inputs.destinations.entity)
    val embedding = edgeBundle(edges.idSet, inputs.rows.entity, EdgeBundleProperties.embedding)
  }

  def run[A: TypeTag, B: TypeTag](
    srcVidAttr: Attribute[A],
    dstVidAttr: Attribute[B],
    srcVidColumn: Attribute[A],
    dstVidColumn: Attribute[B])(implicit m: MetaGraphManager): Output[A, B] = {
    import Scripting._
    val op = {
      if (typeOf[A] =:= typeOf[String] && typeOf[B] =:= typeOf[String])
        new ImportEdgeListForExistingVertexSetFromTable()
      else if (typeOf[A] =:= typeOf[Long] && typeOf[B] =:= typeOf[String])
        new ImportEdgeListForExistingVertexSetFromTableLongString()
      else if (typeOf[A] =:= typeOf[Long] && typeOf[B] =:= typeOf[Long])
        new ImportEdgeListForExistingVertexSetFromTableLongLong()
      else assert(
        false,
        "ImportEdgeListForExistingVertexSetFromTableBase is not supported for" +
          s" ${typeOf[A]} + ${typeOf[B]}.")
    }.asInstanceOf[ImportEdgeListForExistingVertexSetFromTableBase[A, B]]
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
    dstVidColumn: Attribute[_])(implicit m: MetaGraphManager): Output[_, _] = {
    val ta = srcVidAttr.typeTag
    val tb = dstVidAttr.typeTag
    runtimeSafeHelper(srcVidAttr, dstVidAttr, srcVidColumn, dstVidColumn)(ta, tb, m)
  }

  private def runtimeSafeHelper[A: TypeTag, B: TypeTag](
    srcVidAttr: Attribute[_],
    dstVidAttr: Attribute[_],
    srcVidColumn: Attribute[_],
    dstVidColumn: Attribute[_])(implicit m: MetaGraphManager): Output[_, _] = {
    run(
      srcVidAttr.runtimeSafeCast[A],
      dstVidAttr.runtimeSafeCast[B],
      srcVidColumn.runtimeSafeCast[A],
      dstVidColumn.runtimeSafeCast[B])
  }

  def resolveEdges[A: reflect.ClassTag: Ordering, B: reflect.ClassTag: Ordering](
    unresolvedEdges: UniqueSortedRDD[ID, (A, B)],
    srcVidAttr: AttributeData[A],
    dstVidAttr: AttributeData[B]): UniqueSortedRDD[ID, Edge] = {

    val partitioner = unresolvedEdges.partitioner.get

    val srcNameToVid = srcVidAttr.rdd
      .map { case (k, v) => v -> k }
      .assertUniqueKeys(partitioner)
    val dstNameToVid = {
      if (srcVidAttr.gUID == dstVidAttr.gUID)
        srcNameToVid.asInstanceOf[UniqueSortedRDD[B, ID]]
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
abstract class ImportEdgeListForExistingVertexSetFromTableBase[A: reflect.ClassTag: Ordering, B: reflect.ClassTag: Ordering]
    extends TypedMetaGraphOp[Input[A, B], Output[A, B]] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input[A, B]()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[A, B],
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
  extends ImportEdgeListForExistingVertexSetFromTableBase[String, String]

object ImportEdgeListForExistingVertexSetFromTableLongString extends OpFromJson {
  def fromJson(j: JsValue) = new ImportEdgeListForExistingVertexSetFromTableLongString()
}
case class ImportEdgeListForExistingVertexSetFromTableLongString()
  extends ImportEdgeListForExistingVertexSetFromTableBase[Long, String]

object ImportEdgeListForExistingVertexSetFromTableLongLong extends OpFromJson {
  def fromJson(j: JsValue) = new ImportEdgeListForExistingVertexSetFromTableLongLong()
}
case class ImportEdgeListForExistingVertexSetFromTableLongLong()
  extends ImportEdgeListForExistingVertexSetFromTableBase[Long, Long]
