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
  val SetListBruteForceLimit = 70
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
  @transient lazy val outputSig = AttributeSignature
      .empty.addAttribute[Int](outputAttribute).signature

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
    // We cannot use a prefix longer than minOverlap.
    for (iteration <- (2 to minOverlap)) {
      // Move over short lists.
      short ++= long.filter(_._2.size <= SetOverlap.SetListBruteForceLimit)
      long = long.filter(_._2.size > SetOverlap.SetListBruteForceLimit)
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
    // Accept the remaining large set lists. We cannot split them further.
    short ++= long

    val edges: rdd.RDD[Edge[DenseAttributes]] = short.flatMap({
      case (prefix, sets) => edgesFor(prefix, sets)
    })
    // Wrap the vertex RDD in a UnionRDD. This way it can have a distinct name.
    val vertices = sc.union(inputData.vertices)
    return new SimpleGraphData(target, vertices, edges)
  }

  def vertexAttributes(input: Seq[BigGraph]) = input.head.vertexAttributes

  def edgeAttributes(inputGraphSpecs: Seq[BigGraph]) = outputSig

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
  @transient private lazy val outputMaker = outputSig.maker
  @transient private lazy val outputIdx = outputSig.writeIndex[Int](outputAttribute)
  // Not a method, to avoid serializing the whole class.
  protected val DA = (overlap: Int) => {
    val da = outputMaker.make()
    da.set(outputIdx, overlap)
    da
  }

  // Intersection size calculator. The same a-b pair will show up under multiple
  // prefixes if they have more nodes in common than the prefix length. To avoid
  // reporting them multiple times, SortedIntersectionSize() returns 0 unless
  // `pref` is a prefix of the overlap.
  protected def SortedIntersectionSize(
    a: Array[Long], b: Array[Long], pref: Seq[Long]): Int = {
    var ai = 0
    var bi = 0
    val prefi = pref.iterator
    var res = 0
    while (ai < a.length && bi < b.length) {
      if (a(ai) == b(bi)) {
        if (prefi.hasNext && a(ai) != prefi.next) {
          return 0  // `pref` is not a prefix of the overlap.
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
