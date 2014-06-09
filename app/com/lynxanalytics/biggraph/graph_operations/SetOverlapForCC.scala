package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes

class SetOverlapForCC(
    attribute: String,
    minOverlapFn: (Int, Int) => Int) extends GraphOperation {

  // Set-valued attributes are represented as sorted Array[Long].
  type Set = Array[Long]
  // When dealing with multiple sets, they are identified by their VertexIds.
  type Sets = Seq[(VertexId, Array[Long])]

  @transient private lazy val outputSig = AttributeSignature.empty
  @transient private lazy val outputMaker = outputSig.maker

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
    val partitioner = runtimeContext.defaultPartitioner
    var byMemeberNode = sets
      .flatMap { case (sid, set) => set.map(i => (i, (sid, set))) }
      .groupByKey(partitioner)
    val edges: rdd.RDD[Edge[DenseAttributes]] = byMemeberNode.flatMap {
      case (vid, sets) => edgesFor(vid, sets)
    }
    return new SimpleGraphData(target, inputData.vertices, edges)
  }

  def vertexAttributes(inputGraphSpecs: Seq[BigGraph]) = inputGraphSpecs.head.vertexAttributes

  def edgeAttributes(inputGraphSpecs: Seq[BigGraph]) = outputSig

  override def targetProperties(inputGraphSpecs: Seq[BigGraph]) =
    new BigGraphProperties(symmetricEdges = true)

  private def hasEnoughIntersection(a: Array[Long],
                                    b: Array[Long],
                                    minOverlap: Int): Option[Long] = {
    var ai = 0
    var bi = 0
    var res = 0
    var smallest = Long.MaxValue
    while (ai < a.length && bi < b.length) {
      if (a(ai) == b(bi)) {
        res += 1
        ai += 1
        bi += 1
        if (smallest > a(ai)) smallest = a(ai)
        if (res >= minOverlap) return Some(smallest)
      } else if (a(ai) < b(bi)) {
        ai += 1
      } else {
        bi += 1
      }
    }
    return None
  }

  def edgesFor(vid: Long, sets: Sets): Seq[Edge[DenseAttributes]] = {
    val res = mutable.Buffer[Edge[DenseAttributes]]()
    val idxa = (1 until sets.size).toArray
    def minimalElementInIs(current: Int, other: Int): Option[Long] = {
      val cs = sets(idxa(current))._2
      val os = sets(idxa(other))._2
      hasEnoughIntersection(cs, os, minOverlapFn(cs.size, os.size))
    }
    def addEdges(current: Int, other: Int): Unit = {
      res += new Edge(sets(idxa(current))._1, sets(idxa(other))._1, outputMaker.make)
    }

    var idxs = idxa.size
    while (idxs > 0) {
      val todo = mutable.Queue[Int]()
      todo.enqueue(idxa(0))
      while (!todo.isEmpty) {
        val current = todo.dequeue
        var writeIdx = 0
        for (readIdx <- 0 until idxs) {
          val other = idxa(readIdx)
          if (current != other) {
            minimalElementInIs(current, other) match {
              case Some(minimal) => {
                todo.enqueue(other)
                if (minimal >= vid) addEdges(current, other)
              }
              case None => {
                idxa(writeIdx) = other
                writeIdx += 1
              }
            }
          }
        }
        idxs = writeIdx
      }
    }
    res
  }
}

case class UniformOverlapForCC(
  attribute: String,
  overlapSize: Int) extends SetOverlapForCC(attribute, (a: Int, b: Int) => overlapSize)
