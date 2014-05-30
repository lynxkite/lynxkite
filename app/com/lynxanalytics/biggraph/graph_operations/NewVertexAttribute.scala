package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes
import com.lynxanalytics.biggraph.graph_api.attributes.SignatureExtension

/*
 * Generic operation template for an operation that just computes a new vertex attribute.
 */
abstract class NewVertexAttributeOperation[T]
    extends GraphOperation {
  implicit def tt: TypeTag[T]

  val outputAttribute: String

  // You may need to override this.
  def isSourceListValid(sources: Seq[BigGraph]): Boolean = (sources.size == 1)

  // Override this if you need to compute the attribute (at least for certain vertices) using
  // the source graph as a whole. For vertices that this function does not return any value
  // for computeLocally will be called. Returning null means that all values should be
  // computed locally.
  def computeHollistically(inputData: GraphData,
                           runtimeContext: RuntimeContext): rdd.RDD[(graphx.VertexId, T)] = null

  // Override this if you don't return an attribute value for all vertices in computeHollistically.
  // In that case this function will determine the value of the attribute.
  def computeLocally(vid: graphx.VertexId, da: DenseAttributes): T = ???

  // You shouldn't need to touch anything below here. No. Really. Don't touch!

  def execute(target: BigGraph,
              manager: GraphDataManager): GraphData = {
    val inputGraph = target.sources.head
    val inputData = manager.obtainData(inputGraph)
    val runtimeContext = manager.runtimeContext
    val SignatureExtension(sig, cloner) = vertexExtension(target.sources.head)
    val idx = sig.writeIndex[T](outputAttribute)

    val hollisticValues = computeHollistically(inputData, runtimeContext)
    val vertices =
      if (hollisticValues != null) {
        inputData.vertices.leftOuterJoin(hollisticValues).map {
          case (vid, (da, attrOption)) =>
            (vid, cloner.clone(da).set(idx, attrOption.getOrElse(computeLocally(vid, da))))
        }
      } else {
        inputData.vertices.map {
          case (vid, da) => (vid, cloner.clone(da).set(idx, computeLocally(vid, da)))
        }
      }
    // Wrap the edge RDD in a UnionRDD. This way it can have a distinct name.
    val edges = runtimeContext.sparkContext.union(inputData.edges)
    return new SimpleGraphData(target, vertices, edges)
  }

  private def vertexExtension(input: BigGraph) =
    input.vertexAttributes.addAttribute[T](outputAttribute)

  def vertexAttributes(input: Seq[BigGraph]) =
    vertexExtension(input.head).signature

  def edgeAttributes(input: Seq[BigGraph]) = input.head.edgeAttributes

  override def targetProperties(input: Seq[BigGraph]) = input.head.properties
}
