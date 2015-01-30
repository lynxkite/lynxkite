package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object FindModularPartitioning extends OpFromJson {
  class Input() extends MagicInputSignature {
    val vs = vertexSet
    val edgeIds = vertexSet
    val edges = edgeBundle(vs, vs, idSet = edgeIds)
    val weights = vertexAttribute[Double](edgeIds)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val partitions = vertexSet
    val containedIn = edgeBundle(
      inputs.vs.entity, partitions, properties = EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) = FindModularPartitioning()
}
import FindModularPartitioning._
case class FindModularPartitioning() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vs = inputs.vs.rdd
    val vPart = vs.partitioner.get
    var members: SortedRDD[ID, ID] = vs.mapValuesWithKeys(_._1)
    // Total in and out weight from edges from finalized clusters.
    var weightsFromFinals: SortedRDD[ID, (Double, Double)] =
      rc.sparkContext.emptyRDD[(ID, (Double, Double))].toSortedRDD(vPart)
    var connections: SortedRDD[(ID, ID), Double] = inputs.edges.rdd.sortedJoin(inputs.weights.rdd)
      .map { case (id, (e, w)) => ((e.src, e.dst), w) }
      .reduceBySortedKey(vPart, _ + _)
    val numEdges = connections.values.reduce(_ + _)
    do {
      connections.cache()
      val bySrc = connections
        .map { case ((src, dst), w) => src -> (dst, w) }
        .groupBySortedKey(vPart)
      val byDst = connections
        .map { case ((src, dst), w) => dst -> (src, w) }
        .groupBySortedKey(vPart)
      val friendsData = bySrc.fullOuterJoin(byDst).leftOuterJoin(weightsFromFinals)
        .flatMap {
          case (id, ((dstsOpt, srcsOpt), finalsOpt)) =>
            val dsts = dstsOpt.getOrElse(Seq()).toMap
            val srcs = srcsOpt.getOrElse(Seq()).toMap
            val (finalIn, finalOut) = finalsOpt.getOrElse((0.0, 0.0))
            val outDeg = dsts.values.sum + finalOut
            val inDeg = srcs.values.sum + finalIn
            // We need to add id to make sure we send ourselves our own inDeg/outDeg even if
            // there are no loops.
            val friends: Set[ID] = dsts.keySet.union(srcs.keySet) + id
            friends.iterator.map { friend =>
              // Sending to all my friends (that is vertices connected with any edge and myself)
              friend -> (
                // my ID,
                id -> (
                  // my indegree,
                  inDeg,
                  // my outdegree,
                  outDeg,
                  // his inweight from me,
                  dsts.getOrElse(friend, 0.0),
                  // and his outweight to me.
                  srcs.getOrElse(friend, 0.0)))
            }
        }.groupBySortedKey(vPart)

      // Output value contains:
      //   - new ID or None if we are to be finalized
      //   - outgoing weighted edges we are responsible to output somewhere
      //   - incoming weighted edges we are responsible to output somewhere
      val iterationOutput: SortedRDD[ID, (Option[ID], Seq[(ID, Double)], Seq[(ID, Double)])] =
        friendsData.mapValuesWithKeys {
          case (id, friendsList) =>
            val friends: Map[ID, (Double, Double, Double, Double)] = friendsList.toMap
            val (inDeg, outDeg, _, _) = friends(id)
            // We own friends with higher total degree or those with same degree but higher id. We
            // also own ourselves. We are responsible to output edges between ourselves and each owned
            // friend to the next iteration.
            val ownedFriends = friends.filter {
              case (fid, (fInDeg, fOutDeg, _, _)) =>
                (inDeg + outDeg < fInDeg + fOutDeg) ||
                  ((inDeg + outDeg == fInDeg + fOutDeg) && (id <= fid))
            }
            val outEdges = ownedFriends
              .map { case (id, (_, _, _, outWeight)) => (id, outWeight) }
              .toSeq
              .filter(_._2 != 0.0)
            val inEdges = ownedFriends
              // We reproduce loops only once, above.
              .filter(_._1 != id)
              .map { case (id, (_, _, inWeight, _)) => (id, inWeight) }
              .toSeq
              .filter(_._2 != 0.0)
            val newId = if (ownedFriends.size == friends.size) {
              // Yay, we own all our friends. Now we either merge or become final.
              val deltas = friends.toSeq.filter(_._1 != id).map {
                case (id, (fInDeg, fOutDeg, inWeight, outWeight)) =>
                  val fullEdgeDelta = (inWeight + outWeight) / numEdges
                  val expectationDelta = (fInDeg * outDeg + fOutDeg * inDeg) / numEdges / numEdges
                  (fullEdgeDelta - expectationDelta, id)
              }
              val bestFriend = if (deltas.size > 0) deltas.max else (-1.0, 0l)
              if (bestFriend._1 > 0) {
                // We merge with the best, positive delta friend.
                Some(bestFriend._2)
              } else {
                // Nobody to merge with beneficaly.
                None
              }
            } else {
              // Nay, no cards are dealt for us. We just reproduce the edges for owned friendships.
              Some(id)
            }
            (newId, outEdges, inEdges)
        }.cache()

      val memberMoves = iterationOutput.flatMapValues(_._1).filter { case (from, to) => from != to }
      members = members.sortedLeftOuterJoin(memberMoves).map {
        case (oldPart, (member, newPartOpt)) => newPartOpt.getOrElse(oldPart) -> member
      }.toSortedRDD(vPart)

      connections = iterationOutput.flatMap {
        case (_, (newIdOpt, outs, ins)) =>
          newIdOpt match {
            case Some(newId) =>
              outs.iterator.map { case (target, w) => (newId, target) -> w } ++
                ins.iterator.map { case (src, w) => (src, newId) -> w }
            case None => Iterator()
          }
      }.reduceBySortedKey(vPart, _ + _)

      val newFinalWeights = iterationOutput.flatMap {
        case (_, (newIdOpt, outs, ins)) =>
          newIdOpt match {
            case Some(newId) => Iterator()
            case None =>
              outs.iterator.map { case (target, w) => target -> (w, 0.0) } ++
                ins.iterator.map { case (src, w) => src -> (0.0, w) }
          }
      }
      val allFinalWeights = (newFinalWeights ++ weightsFromFinals)
        .reduceBySortedKey(vPart, { case ((in1, out1), (in2, out2)) => (in1 + in2, out1 + out2) })
      val livePartitions = iterationOutput.filter {
        case (id, (newIdOpt, _, _)) => newIdOpt.map(_ == id).getOrElse(false)
      }
      weightsFromFinals = allFinalWeights.sortedJoin(livePartitions).mapValues(_._1)
    } while (connections.count() > 0)

    val partitions = members.groupByKey.randomNumbered(vPart).mapValues(_._2)
    output(o.partitions, partitions.mapValues(_ => ()))
    output(
      o.containedIn,
      partitions
        .flatMap { case (p, members) => members.map(m => Edge(m, p)) }
        .randomNumbered(vPart))
  }
}
