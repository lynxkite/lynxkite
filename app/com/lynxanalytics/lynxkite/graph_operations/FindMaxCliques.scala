// Finds maximal cliques in the graph via the Bron-Kerbosch algorithm.
package com.lynxanalytics.lynxkite.graph_operations

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd
import scala.collection.mutable

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

object FindMaxCliques extends OpFromJson {
  def fromJson(j: JsValue) = FindMaxCliques(
    (j \ "minCliqueSize").as[Int],
    (j \ "needsBothDirections").as[Boolean])
}
case class FindMaxCliques(
    minCliqueSize: Int,
    needsBothDirections: Boolean = false)
    extends SparkOperation[GraphInput, Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    new Segmentation(inputs.vs.entity)
  }

  override def toJson = Json.obj(
    "minCliqueSize" -> minCliqueSize,
    "needsBothDirections" -> needsBothDirections)

  def execute(
      inputDatas: DataSet,
      o: Segmentation,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val inputPartitioner = inputs.vs.rdd.partitioner.get
    val cug = CompactUndirectedGraph(rc, inputs.es.data, needsBothDirections)
    val numTasks = (rc.sparkContext.defaultParallelism * 5) max inputPartitioner.numPartitions
    val outputPartitioner = new HashPartitioner(numTasks)
    val cliqueLists = computeCliques(
      inputs.vs.data,
      cug,
      rc,
      minCliqueSize,
      numTasks)
    val indexedCliqueLists = cliqueLists.randomNumbered(outputPartitioner)
    output(o.segments, indexedCliqueLists.mapValues(_ => ()))
    output(
      o.belongsTo,
      indexedCliqueLists.flatMap {
        case (cid, vids) => vids.map(vid => Edge(vid, cid))
      }.randomNumbered(outputPartitioner))
  }

  // Implementation of the actual algorithm.

  /*
   * Finds best pivot among given candidates based on degree.
   */
  private def FindPivot(candidates: Seq[ID], fullGraph: CompactUndirectedGraph): ID = {
    return candidates
      .map(id => (id, fullGraph.getNeighbors(id).length))
      .maxBy(_._2)._1
  }

  /*
   * Copies the elements of markedCandidates with index [start, end) that
   * are in neighbors back to markedCandidates starting at position end.
   * Extends markedCandidates if necessary. Returns the new end position.
   */
  private def SmartIntersectNA(
      markedCandidates: mutable.ArrayBuffer[(ID, Boolean)],
      start: Int,
      end: Int,
      neighbours: Seq[ID]): Int = {
    var source = start
    var target = end
    val nit = neighbours.iterator.buffered
    while (source < end && nit.hasNext) {
      val nextMarkedCandidate = markedCandidates(source)
      if (nextMarkedCandidate._1 == nit.head) {
        if (target == markedCandidates.size) {
          markedCandidates += markedCandidates(source)
        } else {
          markedCandidates(target) = markedCandidates(source)
        }
        target += 1
        source += 1
        nit.next()
      } else if (nextMarkedCandidate._1 < nit.head) {
        source += 1
      } else {
        nit.next()
      }
    }
    return target
  }

  /*
   * BK implementation, see:
   * http://en.wikipedia.org/wiki/Bron%E2%80%93Kerbosch_algorithm P and X are
   * stored together in a subsequence of markedCandidates with indexes
   * [start, end), only elements of X are marked by setting the second element
   * of the pair to true. The function is allowed to overwrite elements of
   * markedCandidate starting from end or add new elements at the
   * end. Basically the single ArrayBuffer is used as a stack to store the
   * state of the recursion.
   */
  private def SmartBKNA(
      currentClique: List[ID],
      markedCandidates: mutable.ArrayBuffer[(ID, Boolean)],
      start: Int,
      end: Int,
      fullGraph: CompactUndirectedGraph,
      cliqueCollector: mutable.ArrayBuffer[List[ID]],
      minCliqueSize: Int) {
    if (start == end) {
      if (currentClique.size >= minCliqueSize) cliqueCollector += currentClique
      return
    }
    val pivot = FindPivot(markedCandidates.slice(start, end).map(_._1), fullGraph)
    val pit = fullGraph.getNeighbors(pivot).iterator.buffered
    for (idx <- start until end) {
      val (id, is_excluded) = markedCandidates(idx)
      if (!is_excluded) {
        while (pit.hasNext && pit.head < id) {
          pit.next()
        }
        if (!pit.hasNext || pit.head != id) {
          val neighbours = fullGraph.getNeighbors(id)
          val nextEnd = SmartIntersectNA(
            markedCandidates,
            start,
            end,
            neighbours)
          SmartBKNA(
            id :: currentClique,
            markedCandidates,
            end,
            nextEnd,
            fullGraph,
            cliqueCollector,
            minCliqueSize)
          markedCandidates(idx) = (id, true)
        }
      }
    }
  }

  private def computeCliques(
      g: VertexSetData,
      fullGraph: CompactUndirectedGraph,
      rc: RuntimeContext,
      minCliqueSize: Int,
      numTasks: Int): rdd.RDD[List[ID]] = {
    g.rdd.keys.repartition(numTasks).flatMap(v => {
      val markedCandidates =
        mutable.ArrayBuffer.concat(fullGraph.getNeighbors(v).map(n => (n, n < v)))
      val collector = mutable.ArrayBuffer[List[ID]]()
      SmartBKNA(
        List(v),
        markedCandidates,
        0, // start
        markedCandidates.size, // end
        fullGraph,
        collector,
        minCliqueSize)
      collector
    })
  }
}
