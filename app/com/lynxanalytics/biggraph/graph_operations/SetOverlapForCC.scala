package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd
import scala.collection.mutable
import scala.math

import com.lynxanalytics.biggraph.graph_api._

import org.apache.spark.rdd._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object SetOverlapForCC {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val segments = vertexSet
    val belongsTo = edgeBundle(vs, segments)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val overlaps = edgeBundle(inputs.segments.entity, inputs.segments.entity)
  }
}
import SetOverlapForCC._
abstract class SetOverlapForCC extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = rc.defaultPartitioner

    val bySet = inputs.belongsTo.rdd.values
      .map { case Edge(vId, setId) => setId -> vId }
      .groupByKey(partitioner)
    val byMember = bySet
      .flatMap { case (setId, set) => set.map(vId => (vId, (setId, set.toSeq.sorted.toArray))) }
      .groupByKey(partitioner)
    val edges: RDD[Edge] = byMember.flatMap {
      case (vId, sets) => edgesFor(vId, sets.toSeq)
    }
    output(o.overlaps, edges.randomNumbered(rc.defaultPartitioner))
  }

  // Override this with the actual overlap function implementations
  def minOverlapFn(aSize: Int, bSize: Int): Int

  // Checks if the two sorted array has an intersection of at least minOverlap. If yes,
  // returns the minimal element of the intesection. If no, returns None.
  private def hasEnoughIntersection(a: Array[ID],
                                    b: Array[ID],
                                    minOverlap: Int): Option[Long] = {
    var ai = 0
    var bi = 0
    var res = 0
    var smallest = Long.MaxValue
    while (ai < a.length && bi < b.length) {
      if (a(ai) == b(bi)) {
        res += 1
        if (smallest > a(ai)) smallest = a(ai)
        if (res >= minOverlap) return Some(smallest)
        ai += 1
        bi += 1
      } else if (a(ai) < b(bi)) {
        ai += 1
      } else {
        bi += 1
      }
    }
    return None
  }

  def edgesFor(vid: ID, sets: Seq[(ID, Array[ID])]): Seq[Edge] = {
    val res = mutable.Buffer[Edge]()

    // Array of set indices that still need to be checked when considering the neighbors of
    // a new node. idxs contains the number of valid elements in idxa.
    val idxa = (0 until sets.size).toArray
    var idxs = idxa.size

    def minimalElementInIs(current: Int, other: Int): Option[Long] = {
      val cs = sets(current)._2
      val os = sets(other)._2
      hasEnoughIntersection(cs, os, minOverlapFn(cs.size, os.size))
    }
    def addEdges(current: Int, other: Int): Unit = {
      res += Edge(sets(current)._1, sets(other)._1)
      res += Edge(sets(other)._1, sets(current)._1)
    }

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
                // If minimal < vid, then this edge (or some other path between the two end nodes)
                // will be added in the sets with smaller vid.
                if (minimal >= vid) addEdges(current, other)
                // We found the component of this vertex, no need to check for edges going into
                // it anymore.
              }
              case None => {
                // We still need to consider this vertex, so we copy it over to the start of the
                // array.
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

object UniformOverlapForCC extends OpFromJson {
  def fromJson(j: play.api.libs.json.JsValue) = UniformOverlapForCC((j \ "overlapSize").as[Int])
}
case class UniformOverlapForCC(overlapSize: Int) extends SetOverlapForCC {
  def minOverlapFn(a: Int, b: Int): Int = overlapSize
}

object InfocomOverlapForCC extends OpFromJson {
  def fromJson(j: play.api.libs.json.JsValue) = InfocomOverlapForCC((j \ "adjacencyThreshold").as[Double])
}
case class InfocomOverlapForCC(adjacencyThreshold: Double)
    extends SetOverlapForCC {
  def minOverlapFn(a: Int, b: Int): Int =
    math.ceil(adjacencyThreshold * (a + b) * (a * a + b * b) / (4 * a * b)).toInt
}
