package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import scala.collection.mutable

object CheckClique {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val cliques = vertexSet
    val es = edgeBundle(vs, vs)
    val belongsTo = edgeBundle(vs, cliques)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val validCliques = vertexSet
  }
}
import CheckClique._
case class CheckClique(cliquesToCheck: Option[Set[ID]] = None) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val vertexPartitioner = inputs.vs.rdd.partitioner.get
    val cliquePartitioner = inputs.cliques.rdd.partitioner.get
    val belongsTo = inputs.belongsTo.rdd
    val es = inputs.es.rdd

    val neighbors = es.map { case (_, edge) => edge.src -> edge.dst }
      .groupBySortedKey(vertexPartitioner)
    val vsToCliques = belongsTo.map { case (_, edge) => edge.src -> edge.dst }
      .toSortedRDD(vertexPartitioner)
      
    // filtering is to only check cliques that are in the cliquesToCheck set
    val cliquesToVsWithNs = vsToCliques.sortedLeftOuterJoin(neighbors)
      .filter { case (_, (clique, _)) => cliquesToCheck.map(c => c.contains(clique)).getOrElse(true) }
      .map { case (v, (clique, ns)) => clique -> (v, ns.getOrElse(Iterable())) }
      .groupBySortedKey(cliquePartitioner)

    // for every node in the clique create outgoing and ingoing adjacency sets
    // put the node itself into these sets
    // create the intersection of all the sets, this should be the same as the clique members set
    val valid = cliquesToVsWithNs.filter {
      case (clique, vsToNs) =>
        val members = vsToNs.map(_._1).toSet
        val outSets = mutable.Map[ID, mutable.Set[ID]](members.toSeq.map(m => m -> mutable.Set(m)): _*)
        val inSets = mutable.Map[ID, mutable.Set[ID]](members.toSeq.map(m => m -> mutable.Set(m)): _*)
        vsToNs.foreach {
          case (v, ns) if members.contains(v) =>
            outSets(v) ++= ns
            ns.foreach(n => if (members.contains(n)) inSets(n) += v)
        }
        ((outSets.values.reduceLeft(_ & _) & inSets.values.reduceLeft(_ & _)) == members)
    }

    output(o.validCliques, valid.mapValues(_ => ()))
  }
}
