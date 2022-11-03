// Operation for debugging/validating the results of a clique finding operation.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.rdd.RDD

import scala.collection.mutable

import com.lynxanalytics.biggraph.{logger => log}
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object CheckClique extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val cliques = vertexSet
    val es = edgeBundle(vs, vs)
    val belongsTo = edgeBundle(vs, cliques)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val invalid = scalar[List[ID]] // first 100 invalid clique IDs
  }
  def fromJson(j: JsValue) = {
    val set = (j \ "cliquesToCheck").as[Seq[ID]].toSet
    CheckClique(if (set.nonEmpty) Some(set) else None, (j \ "needsBothDirections").as[Boolean])
  }
}
import CheckClique._
case class CheckClique(cliquesToCheck: Option[Set[ID]] = None, needsBothDirections: Boolean = false)
    extends SparkOperation[Input, Output] {
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  override def toJson = Json.obj(
    "cliquesToCheck" -> cliquesToCheck.getOrElse(Set()).toSeq,
    "needsBothDirections" -> needsBothDirections)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val vertexPartitioner = inputs.vs.rdd.partitioner.get
    val cliquePartitioner = inputs.cliques.rdd.partitioner.get
    val belongsTo = inputs.belongsTo.rdd
      .filter { case (_, edge) => cliquesToCheck.map(c => c.contains(edge.dst)).getOrElse(true) }
    val es = inputs.es.rdd

    val neighborsOut = es.map { case (_, edge) => edge.src -> edge.dst }
      .groupBySortedKey(vertexPartitioner)
    val neighborsIn = es.map { case (_, edge) => edge.dst -> edge.src }
      .groupBySortedKey(vertexPartitioner)
    val vsToCliques = belongsTo.map { case (_, edge) => edge.src -> edge.dst }
      .sort(vertexPartitioner)

    val cliquesToVsWithNs = vsToCliques.sortedLeftOuterJoin(neighborsOut).sortedLeftOuterJoin(neighborsIn)
      .map {
        case (v, ((clique, nsOut), nsIn)) =>
          clique -> (v, nsOut.getOrElse(Iterable()), nsIn.getOrElse(Iterable()))
      }
      .groupBySortedKey(cliquePartitioner)

    // for every node in the clique create outgoing and ingoing adjacency sets
    // put the node itself into these sets
    // create the intersection of all the sets, this should be the same as the clique members set
    val invalid: RDD[ID] =
      cliquesToVsWithNs.filter {
        case (clique, vsToNs) =>
          val members = vsToNs.map(_._1).toSet
          val outSets = mutable.Map[ID, mutable.Set[ID]](members.toSeq.map(m => m -> mutable.Set(m)): _*)
          val inSets = mutable.Map[ID, mutable.Set[ID]](members.toSeq.map(m => m -> mutable.Set(m)): _*)
          for ((v, nsOut, nsIn) <- vsToNs) {
            outSets(v) ++= nsOut
            // if one direction is enough, we only use the outSets
            if (needsBothDirections) inSets(v) ++= nsIn else outSets(v) ++= nsIn
          }
          val inv =
            if (needsBothDirections) {
              (outSets.values.reduceLeft(_ & _) & inSets.values.reduceLeft(_ & _)) != members
            } else {
              outSets.values.reduceLeft(_ & _) != members
            }
          if (inv)
            log.error(s"Clique $clique is not a maximal clique!\nmembers: ${members}\nout neighbor sets: ${outSets}\nin neighbor sets: $inSets")
          inv
      }.keys

    output(o.invalid, invalid.take(100).toList)
  }
}
