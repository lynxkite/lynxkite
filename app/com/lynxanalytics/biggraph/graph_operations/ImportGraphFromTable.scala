// Operations and other classes for importing data from tables.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

object ImportEdgesForExistingVertices extends OpFromJson {
  class Input[A, B] extends MagicInputSignature {
    val rows = vertexSet
    val srcVidColumn = vertexAttribute[A](rows)
    val dstVidColumn = vertexAttribute[B](rows)
    val sources = vertexSet
    val destinations = vertexSet
    val srcVidAttr = vertexAttribute[A](sources)
    val dstVidAttr = vertexAttribute[B](destinations)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input[_, _])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.sources.entity, inputs.destinations.entity)
    val embedding = edgeBundle(edges.idSet, inputs.rows.entity, EdgeBundleProperties.embedding)
  }

  def run[A: TypeTag, B: TypeTag](
    srcVidAttr: Attribute[A],
    dstVidAttr: Attribute[B],
    srcVidColumn: Attribute[A],
    dstVidColumn: Attribute[B])(implicit m: MetaGraphManager): Output = {
    import Scripting._
    val op = ImportEdgesForExistingVertices[A, B]()(SerializableType[A], SerializableType[B])
    op(
      op.srcVidColumn, srcVidColumn)(
        op.dstVidColumn, dstVidColumn)(
          op.srcVidAttr, srcVidAttr)(
            op.dstVidAttr, dstVidAttr).result
  }

  def runtimeSafe[A, B](
    srcVidAttr: Attribute[A],
    dstVidAttr: Attribute[B],
    srcVidColumn: Attribute[_],
    dstVidColumn: Attribute[_])(implicit m: MetaGraphManager): Output = {
    implicit val ta = srcVidAttr.typeTag
    implicit val tb = dstVidAttr.typeTag
    run(
      srcVidAttr,
      dstVidAttr,
      srcVidColumn.runtimeSafeCast[A],
      dstVidColumn.runtimeSafeCast[B])
  }

  def resolveEdges[A: reflect.ClassTag: Ordering, B: reflect.ClassTag: Ordering](
    unresolvedEdges: UniqueSortedRDD[ID, (A, B)],
    srcVidAttr: AttributeData[A],
    dstVidAttr: AttributeData[B])(implicit rc: RuntimeContext): UniqueSortedRDD[ID, Edge] = {

    val edgePartitioner = unresolvedEdges.partitioner.get
    val maxPartitioner = RDDUtils.maxPartitioner(
      edgePartitioner, srcVidAttr.rdd.partitioner.get, dstVidAttr.rdd.partitioner.get)

    val srcNameToVid = srcVidAttr.rdd
      .map(_.swap)
      .assertUniqueKeys(maxPartitioner)
    val dstNameToVid = {
      if (srcVidAttr.gUID == dstVidAttr.gUID)
        srcNameToVid.asInstanceOf[UniqueSortedRDD[B, ID]]
      else
        dstVidAttr.rdd
          .map(_.swap)
          .assertUniqueKeys(maxPartitioner)
    }
    val edgesBySrc = unresolvedEdges.map {
      case (edgeId, (srcName, dstName)) => srcName -> (edgeId, dstName)
    }
    val srcResolvedByDst = HybridRDD.of(edgesBySrc, maxPartitioner, even = true)
      .lookupAndRepartition(srcNameToVid)
      .map { case (srcName, ((edgeId, dstName), srcVid)) => dstName -> (edgeId, srcVid) }

    HybridRDD.of(srcResolvedByDst, maxPartitioner, even = true)
      .lookup(dstNameToVid)
      .map { case (dstName, ((edgeId, srcVid), dstVid)) => edgeId -> Edge(srcVid, dstVid) }
      .sortUnique(edgePartitioner)
  }

  def fromJson(j: JsValue) = {
    val srcType = SerializableType.fromJson(j \ "srcType")
    val dstType = SerializableType.fromJson(j \ "dstType")
    ImportEdgesForExistingVertices()(srcType, dstType)
  }
}
import ImportEdgesForExistingVertices._
case class ImportEdgesForExistingVertices[A: SerializableType, B: SerializableType]()
    extends TypedMetaGraphOp[Input[A, B], Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input[A, B]()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "srcType" -> implicitly[SerializableType[A]].toJson,
    "dstType" -> implicitly[SerializableType[B]].toJson)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    import SerializableType.Implicits._

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

// Legacy class.
object ImportEdgeListForExistingVertexSetFromTable extends OpFromJson {
  def fromJson(j: JsValue) = new ImportEdgeListForExistingVertexSetFromTable
}
// Use the new implementation, but without changing the serialized form.
// This keeps the GUID unchanged and avoids recomputation.
class ImportEdgeListForExistingVertexSetFromTable
    extends ImportEdgesForExistingVertices[String, String]()(
      SerializableType[String], SerializableType[String]) {
  override def toJson = Json.obj()
}
