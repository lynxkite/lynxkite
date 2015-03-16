package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

/* Tries to find a partitioning of the graph with high modularity.
 *
 * Iteratively merges vertices as long as it finds vertex pairs whose merge makes the
 * modularity higher.
 */
object FindModularPartitioning extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, edges) = graph
    val weights = edgeAttribute[Double](edges)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val partitions = vertexSet
    val belongsTo = edgeBundle(
      inputs.vs.entity, partitions, properties = EdgeBundleProperties.partialFunction)
  }
  def fromJson(j: JsValue) = FindModularPartitioning()
}
import FindModularPartitioning._
case class FindModularPartitioning() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  @annotation.tailrec
  private def findModularRefinement(
    // Partion ID to member IDs for the partitioning we want to refine.
    members: SortedRDD[ID, ID],
    // For non-finalized partitions, gives the total in and out weight of edges from/to finalized
    // partitions.
    weightsFromFinals: SortedRDD[ID, (Double, Double)],
    // Gives total edge weight going from partion A to partition B.
    connections: SortedRDD[(ID, ID), Double],
    numEdgesOpt: Option[Double]): SortedRDD[ID, ID] = {

    connections.cache()
    val numEdges = numEdgesOpt.getOrElse(connections.values.reduce(_ + _))
    if (connections.count() == 0) members
    else {
      val vPart = members.partitioner.get
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
              val deltas = friendsList.filter(_._1 != id).map {
                case (id, (fInDeg, fOutDeg, inWeight, outWeight)) =>
                  val fullEdgeDelta = (inWeight + outWeight) / numEdges
                  val expectationDelta = (fInDeg * outDeg + fOutDeg * inDeg) / numEdges / numEdges
                  (fullEdgeDelta - expectationDelta, id)
              }
              val bestFriend = if (deltas.size > 0) deltas.max else (-1.0, 0L)
              if (bestFriend._1 > 0) {
                // We merge with the best, positive delta friend.
                Some(bestFriend._2)
              } else {
                // Nobody to merge with beneficially.
                None
              }
            } else {
              // Nay, no cards are dealt for us. We just reproduce the edges for owned friendships.
              Some(id)
            }
            (newId, outEdges, inEdges)
        }.cache()

      val memberMoves = iterationOutput.flatMapValues(_._1).filter { case (from, to) => from != to }
      val newMembers = members.sortedLeftOuterJoin(memberMoves).map {
        case (oldPart, (member, newPartOpt)) => newPartOpt.getOrElse(oldPart) -> member
      }.toSortedRDD(vPart)

      val newConnections = iterationOutput.flatMap {
        case (_, (newIdOpt, outs, ins)) =>
          newIdOpt match {
            case Some(newId) =>
              outs.iterator.map { case (dst, w) => (newId, dst) -> w } ++
                ins.iterator.map { case (src, w) => (src, newId) -> w }
            case None => Iterator()
          }
      }.reduceBySortedKey(vPart, _ + _)

      val newFinalWeights = iterationOutput.flatMap {
        case (_, (newIdOpt, outs, ins)) =>
          newIdOpt match {
            case Some(newId) => Iterator()
            case None =>
              outs.iterator.map { case (dst, w) => dst -> (w, 0.0) } ++
                ins.iterator.map { case (src, w) => src -> (0.0, w) }
          }
      }
      val allFinalWeights = (newFinalWeights ++ weightsFromFinals)
        .reduceBySortedKey(vPart, { case ((in1, out1), (in2, out2)) => (in1 + in2, out1 + out2) })
      val livePartitions = iterationOutput.filter {
        case (id, (newIdOpt, _, _)) => newIdOpt.map(_ == id).getOrElse(false)
      }
      val newWeightsFromFinals = allFinalWeights.sortedJoin(livePartitions).mapValues(_._1)
      findModularRefinement(newMembers, newWeightsFromFinals, newConnections, Some(numEdges))
    }
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vs = inputs.vs.rdd
    val vPart = vs.partitioner.get
    val members = findModularRefinement(
      members = vs.mapValuesWithKeys(_._1),
      weightsFromFinals = rc.sparkContext.emptyRDD[(ID, (Double, Double))].toSortedRDD(vPart),
      connections = inputs.edges.rdd.sortedJoin(inputs.weights.rdd)
        .map { case (id, (e, w)) => ((e.src, e.dst), w) }
        .reduceBySortedKey(vPart, _ + _),
      numEdgesOpt = None)

    val partitions = members.groupByKey.randomNumbered(vPart).mapValues(_._2)
    output(o.partitions, partitions.mapValues(_ => ()))
    output(
      o.belongsTo,
      partitions
        .flatMap { case (p, members) => members.map(m => Edge(m, p)) }
        .randomNumbered(vPart))
  }
}
