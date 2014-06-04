package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd
import scala.collection.immutable
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes
import com.lynxanalytics.biggraph.spark_util.RDDUtils

trait GraphTransformation extends MetaGraphOperation {
  override def inputVertexSetNames = Set("input")
  override def inputEdgeBundleDefs = Map("input" -> ("input", "input"))
  override def outputVertexSetNames = Set("output")
  override def outputEdgeBundleDefs = Set("output" -> ("output", "output"), "link" -> ("input", "output"))
}

case class DataSet(vertexSets: Map[String, VertexSetData] = Map(),
                   edgeBundles: Map[String, EdgeBundleData] = Map(),
                   vertexAttributes: Map[String, VertexAttributeData[_]] = Map(),
                   edgeAttributes: Map[String, EdgeAttributeData[_]] = Map()) {
  // Convenience constructor.
  def this(inst: MetaGraphOperationInstance,
           vertexSets: Map[String, VertexRDD] = Map(),
           edgeBundles: Map[String, EdgeBundleRDD] = Map(),
           vertexAttributes: Map[String, AttributeRDD[_]] = Map(),
           edgeAttributes: Map[String, AttributeRDD[_]] = Map()) =
    this(
      vertexSets.map { case (name, rdd) => VertexSetData(inst.vertexSet(name), rdd) },
      edgeBundles.map { case (name, rdd) => EdgeBundleData(inst.edgeBundle(name), rdd) },
      vertexAttributes.map { case (name, rdd) => VertexAttributeData(inst.vertexAttribute(name), rdd) },
      edgeAttributes.map { case (name, rdd) => EdgeAttriuteData(inst.edgeAttribute(name), rdd) }
    )
}

trait Execution {
  def execute(inst: MetaGraphOperationInstance, dataManager: DataManager): DataSet

  def createInstance(
    inputVertexSets: Map[String, VertexSet],
    inputEdgeBundles: Map[String, EdgeBundle],
    inputVertexAttributes: Map[String, VertexAttribute[_]],
    inputEdgeAttributes: Map[String, EdgeAttribute[_]]
  ): MetaGraphOperationInstance = {
    MetaGraphOperationResult(this, inputVertexSets, inputEdgeBundles, inputVertexAttributes, inputEdgeAttributes)
  }
}

case class MetaGraphOperationResult(
  val operation: MetaGraphOperation with Execution,
  val inputVertexSets: Map[String, VertexSet],
  val inputEdgeBundles: Map[String, EdgeBundle],
  val inputVertexAttributes: Map[String, VertexAttribute[_]],
  val inputEdgeAttributes: Map[String, EdgeAttribute[_]]
) extends MetaGraphOperationInstance {
  override def vertexSetForName(name: String): VertexSet = {
    assert(operation.inputVertexSetNames.contains(name)
           || operation.outputVertexSetNames.contains(name),
           s"No vertex set called $name.")
    return VertexSet(operation, name)
  }
  override def edgeBundleForName(name: String): EdgeBundle = {
    assert(operation.inputEdgeBundleDefs.contains(name)
           || operation.outputEdgeBundleDefs.contains(name),
           s"No edge bundle called $name.")
    return EdgeBundle(operation, name)
  }
  override def vertexAttribute[T : TypeTag](name: String): VertexAttribute[T] = {
    assert(operation.inputVertexAttributeDefs.contains(name)
           || operation.outputVertexAttributeDefs.contains(name),
           s"No vertex attribute called $name.")
    return VertexAttribute[T](operation, name)
  }
  override def edgeAttribute[T : TypeTag](name: String): EdgeAttribute[T] = {
    assert(operation.inputEdgeAttributeDefs.contains(name)
           || operation.outputEdgeAttributeDefs.contains(name),
           s"No edge attribute called $name.")
    return EdgeAttribute[T](operation, name)
  }
  override def execute(dataManager: DataManager): DataSet =
    operation.execute(this, dataManager)
}

case class FindMaxCliques(minCliqueSize: Int) extends GraphTransformation with Execution {
  def execute(inst: MetaGraphOperationInstance, manager: DataManager): DataSet = {
    val runtimeContext = manager.runtimeContext
    val sc = runtimeContext.sparkContext
    val cug = CompactUndirectedGraph(inst.edgeBundle("input"))
    val cliqueLists = computeCliques(
      inst.vertexSet("input"),
      cug,
      sc,
      minCliqueSize,
      runtimeContext.numAvailableCores * 5)
    val indexedCliqueLists = RDDUtils.fastNumbered(cliqueLists)
    val vertices = indexedCliqueLists.mapValues(x => Unit)
    val edges = RDDUtils.empty[EdgeBundleRDD]
    val links = indexedCliqueLists.flatMap {
      case (cid, vids) => vids.map(vid => (42, (vid, cid)))
    }
    return DataSet(inst, Map("output" -> vertices), Map("output" -> edges, "link" -> links))
  }

  // TODO: Put this into the EdgeBundle?
  override def targetProperties(inputGraphSpecs: Seq[BigGraph]) =
    new BigGraphProperties(symmetricEdges = true)

  // Implementation of the actual algorithm.

  /*
   * Finds best pivot among given candidates based on degree.
   */
  private def FindPivot(candidates: Seq[VertexId], fullGraph: CompactUndirectedGraph): VertexId = {
    return candidates
      .map(id => (id, fullGraph.getNeighbors(id).length))
      .maxBy(_._2)._1
  }

  /*
   * Copies the elements of markedCandidates with index [start, end) that
   * are in neighbors back to markedCandidates starting at position end.
   * Extends markedCandidates if necessary. Returns the new end position.
   */
  private def SmartIntersectNA(markedCandidates: mutable.ArrayBuffer[(VertexId, Boolean)],
                               start: Int,
                               end: Int,
                               neighbours: Seq[VertexId]): Int = {
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
  private def SmartBKNA(currentClique: List[VertexId],
                        markedCandidates: mutable.ArrayBuffer[(VertexId, Boolean)],
                        start: Int,
                        end: Int,
                        fullGraph: CompactUndirectedGraph,
                        cliqueCollector: mutable.ArrayBuffer[List[VertexId]],
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
            markedCandidates, start, end, neighbours)
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

  private def computeCliques(g: VertexSetData,
                             cug: CompactUndirectedGraph,
                             sc: spark.SparkContext,
                             minCliqueSize: Int,
                             numTasks: Int): rdd.RDD[List[VertexId]] = {
    val broadcastGraph = sc.broadcast(cug)
    g.rdd.map(_._1).repartition(numTasks).flatMap(
      v => {
        val fullGraph = broadcastGraph.value
        val markedCandidates =
          mutable.ArrayBuffer.concat(fullGraph.getNeighbors(v).map(n => (n, n < v)))
        val collector = mutable.ArrayBuffer[List[VertexId]]()
        SmartBKNA(
          List(v),
          markedCandidates,
          0, // start
          markedCandidates.size, // end
          fullGraph,
          collector,
          minCliqueSize)
        collector
      }
    )
  }
}
