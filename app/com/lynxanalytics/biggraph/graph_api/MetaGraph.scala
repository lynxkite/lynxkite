package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

import attributes.AttributeSignature

sealed trait MetaGraphComponent extends Serializable {
  val gUID: UUID
  val sourceOperation: MetaGraphOperationInstance
  val attributes: AttributeSignature
}

case class VertexSet(val sourceOperation: MetaGraphOperationInstance,
                     val name: String) extends MetaGraphComponent {
  // Implement from source operation's GUID, name and the fact that this is a vertex set.
  @transient lazy val gUID = ???
  @transient lazy val attributes: AttributeSignature = sourceOperation.vertexSetAttributes(name)
}

case class EdgeBundle(val sourceOperation: MetaGraphOperationInstance,
                      val name: String) extends MetaGraphComponent {
  // Implement from source operation's GUID, name and the fact that this is an edge bundle.
  @transient lazy val gUID = ???
  @transient lazy val attributes: AttributeSignature = sourceOperation.edgeBundleAttributes(name)

  @transient lazy val (srcName, dstName) = sourceOperation.operation.outputEdgeBundleDefs(name)
  @transient lazy val srcVertexSet: VertexSet = sourceOperation.vertexSetForName(srcName)
  @transient lazy val dstVertexSet: VertexSet = sourceOperation.vertexSetForName(dstName)
}

trait MetaGraphOperation extends Serializable {
  // Names of vertex set inputs for this operation.
  def inputVertexSetNames: Set[String]

  // Names of input bundles together with their source and desination vertex set names (which all
  // must be elements of inputVertexSetNames).
  def inputEdgeBundleDefs: Map[String, (String, String)]

  // Names of vertex sets newly created by this operation.
  // (outputVertexSetNamess /\ inputVertexSetNames needs to be empty).
  def outputVertexSetNames: Set[String]

  // Names of bundles newly created by this operation together with their source and desination
  // vertex set names (which all must be elements of inputVertexSetNames \/ outputVertexSetNames).
  def outputEdgeBundleDefs: Map[String, (String, String)]

  // Checks whether the complete input signature is valid for this operation.
  // Concrete MetaGraphOperation instances may either override this method or
  // validateInputVertexSet and validateInputEdgeBundle.
  def validateInput(inputVertexSets: Map[String, VertexSet],
                    inputEdgeBundles: Map[String, EdgeBundle]): Boolean = {
    validateInputStructure(inputVertexSets, inputEdgeBundles) &&
      inputVertexSets.forall {
        case (name, vertexSet) => validateInputVertexSet(name, vertexSet)
      } &&
      inputEdgeBundles.forall {
        case (name, edgeBundle) => validateInputEdgeBundle(name, edgeBundle)
      }
  }

  // Validates the signature of a single input VertexSet.
  def validateInputVertexSet(name: String, vertexSet: VertexSet): Boolean = true

  // Validates the signature of a single input EdgeBundle.
  def validateInputEdgeBundle(name: String, edgeBundle: EdgeBundle): Boolean = true

  protected def validateInputStructure(inputVertexSets: Map[String, VertexSet],
                                       inputEdgeBundles: Map[String, EdgeBundle]): Boolean = {
    // Checks that all required inputs are defined and that bundles connect the right sets.
    // Implemented here....
    true
  }

  val gUID: UUID

  def createInstance(inputVertexSets: Map[String, VertexSet],
                     inputEdgeBundles: Map[String, EdgeBundle]): MetaGraphOperationInstance
}

/*
 * Base class for concrete instances of MetaGraphOperations. An instance of an operation is
 * the operation together with concrete input vertex sets and edge bundles.
 */
abstract class MetaGraphOperationInstance(
    val operation: MetaGraphOperation,
    val inputVertexSets: Map[String, VertexSet],
    val inputEdgeBundles: Map[String, EdgeBundle]) extends Serializable {

  val gUID = ??? // TODO implement here from guids of inputs and operation.

  def vertexSetForName(name: String): VertexSet
  def edgeBundleForName(name: String): EdgeBundle

  def vertexSetAttributes(name: String): AttributeSignature
  def edgeBundleAttributes(name: String): AttributeSignature

  def execute(dataManager: DataManager): (Map[String, VertexSetData], Map[String, EdgeBundleData])
}

trait MetaGraphManager {
  def apply(operationInstance: MetaGraphOperationInstance): Unit
  def vertexSetForGUID(gUID: UUID): VertexSet
  def edgeBundleForGUID(gUID: UUID): EdgeBundle

  def incommingBundles(vertexSet: VertexSet): Seq[EdgeBundle]
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle]
  def dependentOperations(component: MetaGraphComponent): Seq[MetaGraphOperationInstance]
}

trait VertexSetData {
  val vertexSet: VertexSet

  val vertices: VertexRDD
}

trait EdgeBundleData {
  val edgeBundle: EdgeBundle

  // We are reusing the Edge and Triplet RDDs here, but the big semmantic difference
  // is that source and destination ids are comming from a different id space.
  // Also, for triplets, dstAttr and srcAttr has different signature
  // (edgeBundle.srcGraph.vertexAttributes and edgeBundle.dstGraph.vertexAttributes
  // respectively).
  val edges: EdgeRDD
  val triplets: TripletRDD
}

trait DataManager {
  def obtainVertexSetData(vertexSet: VertexSet): VertexSetData
  def obtainEdgeBundleData(edgeBundle: EdgeBundle): EdgeBundleData

  // Saves the given component's data to disk.
  def saveDataToDisk(component: MetaGraphComponent)

  // Returns information about the current running enviroment.
  // Typically used by operations to optimize their execution.
  def runtimeContext: RuntimeContext
}
