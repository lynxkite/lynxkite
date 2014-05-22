package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes

// Generates edges between vertices by the amount of overlap in an attribute.
object SetOverlap {
  // Maximum number of sets to be O(n^2) compared.
  val SetListSizeLimit = 70
}
case class SetOverlap(
    attribute: String,
    minOverlap: Int) extends GraphOperation {

  // Set-valued attributes are represented as sorted Array[Long].
  type Set = Array[Long]
  // When dealing with multiple sets, they are identified by their VertexIds.
  type Sets = Seq[(VertexId, Array[Long])]
  // The generated edges have a single attribute, the overlap size.
  val outputAttribute = attribute + "_overlap"
  @transient lazy val sig = AttributeSignature
      .empty.addAttribute[Double](outputAttribute).signature

  def isSourceListValid(sources: Seq[BigGraph]): Boolean = (
    sources.size == 1
    && sources.head.vertexAttributes.canRead[Set](attribute)
  )

  def execute(target: BigGraph,
              manager: GraphDataManager): GraphData = {
    val inputGraph = target.sources.head
    val inputData = manager.obtainData(inputGraph)
    val runtimeContext = manager.runtimeContext
    val sc = runtimeContext.sparkContext
    val inputIdx = inputGraph.vertexAttributes.readIndex[Set](attribute)
    val sets = inputData.vertices.mapValues(_(inputIdx))
    val cores = runtimeContext.numAvailableCores
    val partitioner = new spark.HashPartitioner(cores * 5)
    type SetsByPrefix = rdd.RDD[(Seq[Long], Sets)]
    // Start with prefixes of length 1.
    var short: SetsByPrefix = new rdd.EmptyRDD[(Seq[Long], Sets)](sc)
    var long: SetsByPrefix =
      sets.flatMap({
        case (vid, set) => set.map(i => (Seq(i), (vid, set)))
      }).groupByKey(partitioner)
    // Increase prefix length until all set lists are short.
    for (iteration <- (2 to minOverlap)) {
      // Move over short lists.
      short ++= long.filter(_._2.size <= SetOverlap.SetListSizeLimit)
      long = long.filter(_._2.size > SetOverlap.SetListSizeLimit)
      // Increase prefix length.
      long = long.flatMap({
        case (prefix, sets) => sets.flatMap({
          case (vid, set) => {
            set.filter(node => node > prefix.last)
               .map(next => (prefix :+ next, (vid, set)))
          }
        })
      }).groupByKey(partitioner)
    }
    // We cannot use a prefix longer than minOverlap. Accept the remaining sets.
    short ++= long

    val edges: rdd.RDD[Edge[DenseAttributes]] = short.flatMap({
      case (prefix, sets) => edgesFor(prefix, sets)
    })
    return new SimpleGraphData(target, inputData.vertices, edges)
  }

  def vertexAttributes(input: Seq[BigGraph]) = input.head.vertexAttributes

  def edgeAttributes(inputGraphSpecs: Seq[BigGraph]) = sig

  // Generates the edges for a set of sets. This is O(n^2), but the set should
  // be small.
  protected def edgesFor(prefix: Seq[Long], sets: Sets): Seq[Edge[DenseAttributes]] = {
    for {
      (vid1, set1) <- sets
      (vid2, set2) <- sets
      overlap = SortedIntersectionSize(set1, set2, prefix)
      if vid1 != vid2 && overlap >= minOverlap
    } yield Edge(vid1, vid2, DA(overlap))
  }

  // Set up for writing the result into DenseAttributes.
  @transient private lazy val outputMaker = sig.maker
  @transient private lazy val outputIdx = sig.writeIndex[Double](outputAttribute)
  // Not a method, to avoid serializing the whole class.
  protected val DA = (overlap: Int) => {
    val da = outputMaker.make()
    da.set(outputIdx, overlap.toDouble)
    da
  }

  // Fast intersection size calculator.
  protected def SortedIntersectionSize(
    a: Array[Long], b: Array[Long], pref: Seq[Long]): Int = {
    var ai = 0
    var bi = 0
    val prefi = pref.iterator.buffered
    var res = 0
    while (ai < a.length && bi < b.length) {
      if (a(ai) == b(bi)) {
        if (prefi.hasNext && a(ai) != prefi.head) {
          return 0
        }
        res += 1
        if (prefi.hasNext) {
          prefi.next
        }
        ai += 1
        bi += 1
      } else if (a(ai) < b(bi)) {
        if (prefi.hasNext && b(bi) > prefi.head) {
          return 0
        }
        ai += 1
      } else {
        if (prefi.hasNext && a(ai) > prefi.head) {
          return 0
        }
        bi += 1
      }
    }
    return res
  }
}

// A faster version, that omits many edges, but the connected components will
// still be the same in the resulting graph.
class SetOverlapForConnectedComponents(
    attribute: String,
    minOverlap: Int) extends SetOverlap(attribute, minOverlap) {
  override def edgesFor(prefix: Seq[Long], sets: Sets): Seq[Edge[DenseAttributes]] = {
    if (prefix.size == minOverlap) {
      val center: Long = sets.head._1
      sets.flatMap({
        case (vid, set) => List(Edge(vid, center, DA(minOverlap)),
                                Edge(center, vid, DA(minOverlap)))
      })
    } else {
      super.edgesFor(prefix, sets)
    }
  }
}
