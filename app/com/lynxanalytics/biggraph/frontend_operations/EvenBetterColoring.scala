
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import com.lynxanalytics.biggraph.graph_api.{ DataSet, OutputBuilder, RuntimeContext, _ }
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

import scala.util.control.Breaks

object EvenBetterColoring extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val coloring = vertexAttribute[Double](inputs.vs.entity)

  }
  def fromJson(j: JsValue) = EvenBetterColoring()
}
import EvenBetterColoring._
case class EvenBetterColoring()
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  // !!!!!!!!!!!!!!!!!!!!!                     EZZEL VALSZEG MEG KENE VALAMIT CSINALNI                    !!!!!!!!!!!!!
  //                                                            ||
  //                                                            \/
  //override def toJson = Json.obj("dampingFactor" -> dampingFactor, "iterations" -> iterations)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val vertices = inputs.vs.rdd
    val vertexPartitioner = vertices.partitioner.get
    val edgePartitioner = edges.partitioner.get

    val edgesWithoutID = edges.map { case (id, e) => (e.src, e.dst) }.
      filter { case (src, dst) => src != dst }.distinct()

    val degreeWithoutIsolatedVertices = (edgesWithoutID ++ edgesWithoutID.map { case (src, dst) => (dst, src) }).
      groupBySortedKey(vertexPartitioner).mapValues(l => l.size.toDouble)

    val degree = (degreeWithoutIsolatedVertices ++ vertices.mapValues(_ => 0)).reduceByKey(_ max _).asUniqueSortedRDD
    val convexOrdering = degree.mapValues(degr => {
      val rnd = scala.util.Random; degr * Math.pow(-1, rnd.nextInt(1))
    })

    val randomOrdering: AttributeRDD[Double] = {
      vertices.mapPartitionsWithIndex({
        (pid, it) =>
          val rnd = new util.Random(pid)
          it.map {
            case (vid, _) => vid -> rnd.nextDouble()
          }
      },
        preservesPartitioning = true).asUniqueSortedRDD
    }

    def truncateOrdering(ordering: AttributeRDD[Double], factor: Long) = {
      val ret = ordering.mapValues { o => o % factor }
      println("truncate has been done")
      ret
    }

    var bestColoringSoFar = vertices.mapValues(_ => vertices.count().toDouble)
    var currentNumberOfColors = vertices.count().toDouble

    def updateNumberOfColors() = {
      currentNumberOfColors = bestColoringSoFar.map { case (id, color) => color }.max()
    }

    def updateBestColorSoFar(ordering: AttributeRDD[Double]) = {
      bestColoringSoFar = pertColor(ordering, currentNumberOfColors)
      updateNumberOfColors()
    }

    def findColoring() = {
      val start = System.currentTimeMillis()
      updateBestColorSoFar(degree)
      val c1 = System.currentTimeMillis()
      println("pertColor with degree ordering has been done for EvenBetterColoring in " + (c1 - start) + " ms")
      updateBestColorSoFar(convexOrdering)
      val c2 = System.currentTimeMillis()
      println("pertColor with convexOrdering has been done for EvenBetterColoring in " + (c2 - c1) + " ms")
      var prevNumberOfColors = currentNumberOfColors
      var cnt = 0
      do {
        updateBestColorSoFar(truncateOrdering(bestColoringSoFar, math.round(currentNumberOfColors / 2)))
        println("needed " + prevNumberOfColors + " colors till now, but found a new coloring with " + currentNumberOfColors + " colors")
        cnt += 1
      } while (prevNumberOfColors > currentNumberOfColors)
      val end = System.currentTimeMillis()
      println("EvenBetterColoring did " + cnt + " truncating in " + (end - c2) + " ms")
      println("EvenBetterColoring ended in " + (end - start) + " miliseconds")
      bestColoringSoFar
    }

    /* pertColor gets an ordering:AttributeRDD which tells us how to direct the edges:
     *   from lower order to the higher one
     * Calculates [pi(v)  = the length of the longest directed path starting with vertex v] for every vertex v and the
     *   vertices with same pi will in the same color class
     * The tooManyColors parameter is optional, by giving it a positive value it can be used to specify a limit
     *   such that if there would be at least that many color classes then the function terminates and doesn't change
     *   the coloring. Else it returns a better coloring:AttributeRDD with the colors for each vertex v (i.e. pi(v))
     */
    def pertColor(ordering: AttributeRDD[Double], tooManyColors: Double = vertices.count()) = {
      val start = System.currentTimeMillis()

      // RDD of (dst, (src, order of src))
      val directedEdges2 = ordering.join(edgesWithoutID).map { case (src, ordDst) => (ordDst._2, (src, ordDst._1)) }
      // RDD of ((src, order of src), (dst, order of dst)
      val directedEdges1 = ordering.join(directedEdges2).
        map { case (dst, ordDst_srcOrdSrc) => (ordDst_srcOrdSrc._2, (dst, ordDst_srcOrdSrc._1)) }
      // RDD of (src, dst) where edges are directed in such a way that order of src < order of dst
      var directedEdges = directedEdges1.map {
        case (srcOrd, dstOrd) => if (srcOrd._2 < dstOrd._2) (srcOrd._1, dstOrd._1)
        else if (srcOrd._2 > dstOrd._2) (dstOrd._1, srcOrd._1)
        else if (srcOrd._1 < dstOrd._1) (srcOrd._1, dstOrd._1)
        else (dstOrd._1, srcOrd._1)
      }

      var c1 = System.currentTimeMillis()
      println("ordering the edges according to the input ordering is done in " + (c1 - start) + " ms")

      var coloringForGivenOrder = vertices.mapValues(_ => 1.0)
      var currentColor = 1.0

      while ((!directedEdges.isEmpty()) && (currentColor < tooManyColors)) {
        println("now finding the " + currentColor + "th color class")
        currentColor += 1.0
        val c2 = System.currentTimeMillis()
        var notYetColored = directedEdges.mapValues(dst => currentColor).distinct
        println("    notYetColored is calculated in " + (c2 - c1) + " ms")
        coloringForGivenOrder = (coloringForGivenOrder ++ notYetColored).reduceByKey(_ max _).asUniqueSortedRDD
        val c3 = System.currentTimeMillis()
        println("    coloringForGivenOrder is calculated in " + (c3 - c2) + " ms")
        directedEdges = directedEdges.map { case (src, dst) => (dst, src) }.join(notYetColored).
          map { case (dst, srcColor) => (srcColor._1, dst) }
        val c4 = System.currentTimeMillis()
        println("    directedEdges is updated in " + (c4 - c3) + " ms")
        println("        now there are " + directedEdges.count() + " edges in directEdges")
        println("In total, the " + currentColor + "th color class has been found in " + (c4 - c1) + " ms")
        c1 = System.currentTimeMillis()
      }
      val end = System.currentTimeMillis()
      if (directedEdges.isEmpty()) {
        println("found the best so far coloring with " + currentColor + " colors in " + (end - start) + " ms"); coloringForGivenOrder
      } else { println("abandoned this ordering in " + (end - start) + " ms"); bestColoringSoFar }
    }

    findColoring()
    output(o.coloring, bestColoringSoFar.sortedRepartition(vertexPartitioner))
  }
}
