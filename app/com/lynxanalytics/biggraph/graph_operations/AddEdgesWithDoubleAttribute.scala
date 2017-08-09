// Predicts edges in a graph that has hyperbolic coordinate attributes.
// Runs PSOGenerator with the already existing coordinates.
// Takes the top X most likely edges.
package com.lynxanalytics.biggraph.graph_operations

import scala.collection.immutable.SortedMap
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object AddEdgesWithDoubleAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val newEdges = edgeBundle(vs, vs)
    val attr = edgeAttribute[Double](newEdges)
  }
  class Output(
      implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val union = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val newAttr = edgeAttribute[Double](union)
  }
  def fromJson(j: JsValue) = AddEdgesWithDoubleAttribute(
    (j \ "defaultvalue").as[Double])
}
import AddEdgesWithDoubleAttribute._
case class AddEdgesWithDoubleAttribute(defaultValue: Double)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "defaultvalue" -> defaultValue)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edgePartitioner = inputs.attr.rdd.partitioner.get
    val newIDsWithAttr = (inputs.es.rdd ++ inputs.newEdges.rdd)
      .map { case (oldid, e) => oldid -> (oldid, e) }
      .randomNumbered(edgePartitioner)
      .map { case (newid, (oldid, edge)) => oldid -> (newid, edge) }
      .sortUnique(edgePartitioner) //TODO this kills the thing
      .sortedLeftOuterJoin(inputs.attr.rdd)
    println("where the")
    val defaultFillWithNewIDs = newIDsWithAttr.map {
      case (oldidtoo, ((oldid, (newid, edge)), attr)) =>
        newid -> (edge, attr.getOrElse(defaultValue))
    }
    val bundleUnion = defaultFillWithNewIDs.map { case (id, (e, attr)) => (id, e) }
      .sortUnique(edgePartitioner)
    output(o.union, bundleUnion)
    val attrUnion = defaultFillWithNewIDs.map { case (id, (e, attr)) => (id, attr) }
      .sortUnique(edgePartitioner)
    output(o.newAttr, attrUnion)
  }
}

