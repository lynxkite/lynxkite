// Generates edges between segments that overlap.
package com.lynxanalytics.lynxkite.graph_operations

import org.apache.spark.rdd._

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

object SetOverlap extends OpFromJson {
  // Maximum number of sets to be O(n^2) compared.
  val SetListBruteForceLimit = 70
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val segments = vertexSet
    val belongsTo = edgeBundle(vs, segments)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val overlaps = edgeBundle(inputs.segments.entity, inputs.segments.entity)
    val overlapSize = edgeAttribute[Double](overlaps)
  }
  def fromJson(j: JsValue) = SetOverlap((j \ "minOverlap").as[Int])
}
import SetOverlap._
case class SetOverlap(minOverlap: Int) extends SparkOperation[Input, Output] {
  // Set-valued attributes are represented as sorted Array[ID].
  type Set = Array[ID]
  // When dealing with multiple sets, they are identified by their VertexIds.
  type Sets = Iterable[(ID, Array[ID])]

  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)
  override def toJson = Json.obj("minOverlap" -> minOverlap)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val belongsTo = inputs.belongsTo.rdd
    val partitioner = belongsTo.partitioner.get

    val sets = belongsTo.values
      .map { case Edge(vId, setId) => setId -> vId }
      .groupByKey(partitioner)

    type SetsByPrefix = RDD[(Seq[ID], Sets)]
    // Start with prefixes of length 1.
    var short: SetsByPrefix = rc.sparkContext.emptyRDD[(Seq[ID], Sets)]
    var long: SetsByPrefix = sets.flatMap {
      case (setId, set) => set.map(i => (Seq(i), (setId, set.toSeq.sorted.toArray)))
    }.groupByKey(partitioner)
    // Increase prefix length until all set lists are short.
    // We cannot use a prefix longer than minOverlap.
    for (iteration <- (2 to minOverlap)) {
      // Move over short lists.
      short ++= long.filter(_._2.size <= SetOverlap.SetListBruteForceLimit)
      long = long.filter(_._2.size > SetOverlap.SetListBruteForceLimit)
      // Increase prefix length.
      long = long.flatMap {
        case (prefix, sets) => sets.flatMap {
            case (setId, set) =>
              set
                .filter(node => node > prefix.last)
                .map(next => (prefix :+ next, (setId, set)))
          }
      }.groupByKey(partitioner)
    }
    // Accept the remaining large set lists. We cannot split them further.
    short ++= long

    val edgesWithOverlaps: RDD[(Edge, Int)] = short.flatMap {
      case (prefix, sets) => edgesFor(prefix, sets)
    }

    val numberedEdgesWithOverlaps = edgesWithOverlaps.randomNumbered(partitioner)

    output(o.overlaps, numberedEdgesWithOverlaps.mapValues(_._1))
    output(o.overlapSize, numberedEdgesWithOverlaps.mapValues(_._2.toDouble))
  }

  // Generates the edges for a set of sets. This is O(n^2), but the set should
  // be small.
  protected def edgesFor(prefix: Seq[ID], sets: Sets): Seq[(Edge, Int)] = {
    val setSeq = sets.toSeq
    for {
      (vid1, set1) <- setSeq
      (vid2, set2) <- setSeq
      overlap = SortedIntersectionSize(set1, set2, prefix)
      if vid1 != vid2 && overlap >= minOverlap
    } yield (Edge(vid1, vid2), overlap)
  }

  // Intersection size calculator. The same a-b pair will show up under multiple
  // prefixes if they have more nodes in common than the prefix length. To avoid
  // reporting them multiple times, SortedIntersectionSize() returns 0 unless
  // `pref` is a prefix of the overlap.
  protected def SortedIntersectionSize(
      a: Array[ID],
      b: Array[ID],
      pref: Seq[ID]): Int = {
    var ai = 0
    var bi = 0
    val prefi = pref.iterator
    var res = 0
    while (ai < a.length && bi < b.length) {
      if (a(ai) == b(bi)) {
        if (prefi.hasNext && a(ai) != prefi.next) {
          return 0 // `pref` is not a prefix of the overlap.
        }
        res += 1
        ai += 1
        bi += 1
      } else if (a(ai) < b(bi)) {
        ai += 1
      } else {
        bi += 1
      }
    }
    return res
  }
}
