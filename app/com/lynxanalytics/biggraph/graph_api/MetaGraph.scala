package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.spark.rdd
import scala.reflect.runtime.universe._

import attributes.AttributeSignature

sealed trait MetaGraphComponent extends Serializable {
  val sourceOperation: MetaGraphOperationInstance
  val name: String
  // Implement from source operation's GUID, name and the actual class of this component.
  val gUID: UUID = ???
}

case class VertexSet(sourceOperation: MetaGraphOperationInstance,
                     name: String) extends MetaGraphComponent

case class EdgeBundle(sourceOperation: MetaGraphOperationInstance,
                      name: String) extends MetaGraphComponent {
  @transient lazy val (srcName, dstName) = sourceOperation.operation.outputEdgeBundleDefs(name)
  @transient lazy val srcVertexSet: VertexSet = sourceOperation.vertexSetForName(srcName)
  @transient lazy val dstVertexSet: VertexSet = sourceOperation.vertexSetForName(dstName)
}

class Attribute[+T: TypeTag] {
  def typeTag: TypeTag[_ <: T] = implicitly[TypeTag[T]]
}

case class VertexAttribute[T: TypeTag](sourceOperation: MetaGraphOperationInstance,
                                       name: String)
    extends Attribute with MetaGraphComponent {
  @transient lazy val vertexSet: VertexSet =
    sourceOperation.vertexSetForName(sourceOperation.operation.outputVertexAttributeDefs(name)._1)
}

case class EdgeAttribute[T: TypeTag](sourceOperation: MetaGraphOperationInstance,
                                     name: String)
    extends Attribute with MetaGraphComponent {
  @transient lazy val edgeBundle: EdgeBundle =
    sourceOperation.edgeBundleForName(sourceOperation.operation.outputEdgeAttributeDefs(name)._1)
}

trait MetaGraphOperation extends Serializable {
  // Names of vertex set inputs for this operation.
  def inputVertexSetNames: Set[String] = Set()

  // Names of input bundles together with their source and desination vertex set names (which all
  // must be elements of inputVertexSetNames).
  def inputEdgeBundleDefs: Map[String, (String, String)] = Map()

  // Names of input vertex attributes together with their corresponding VertexSet names (which all
  // must be elements of inputVertexSetNames) and the types of the attributes.
  def inputVertexAttributeDefs: Map[String, (String, TypeTag[_])] = Map()

  // Names of input edge attributes together with their corresponding EdgeBundle names (which all
  // must be keys of inputEdgeBundleDefs) and the types of the attributes.
  def inputEdgeAttributeDefs: Map[String, (String, TypeTag[_])] = Map()

  // Names of vertex sets newly created by this operation.
  // (outputVertexSetNamess /\ inputVertexSetNames needs to be empty).
  def outputVertexSetNames: Set[String] = Set()

  // Names of bundles newly created by this operation together with their source and desination
  // vertex set names (which all must be elements of inputVertexSetNames \/ outputVertexSetNames).
  def outputEdgeBundleDefs: Map[String, (String, String)] = Map()

  // Names of vertex attributes newly created by this operation together with their corresponding
  // VertexSet names (which all must be elements of inputVertexSetNames \/ outputVertexSetNames)
  // and the types of the attributes.
  def outputVertexAttributeDefs: Map[String, (String, TypeTag[_])] = Map()

  // Names of edge attributes newly created by this operation together with their corresponding
  // EdgeBundle names (which all must be keys of inputEdgeBundleDefs \/ outputEdgeBundleDefs)
  // and the types of the attributes.
  def outputEdgeAttributeDefs: Map[String, (String, TypeTag[_])] = Map()

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

  def createInstance(inputVertexSets: Map[String, VertexSet] = Map(),
                     inputEdgeBundles: Map[String, EdgeBundle] = Map(),
                     inputVertexAttributes: Map[String, VertexAttribute[_]] = Map(),
                     inputEdgeAttributes: Map[String, EdgeAttribute[_]] = Map()): MetaGraphOperationInstance
}

/*
 * Base class for concrete instances of MetaGraphOperations. An instance of an operation is
 * the operation together with concrete input vertex sets and edge bundles.
 */
abstract class MetaGraphOperationInstance(
    val operation: MetaGraphOperation,
    val inputVertexSets: Map[String, VertexSet],
    val inputEdgeBundles: Map[String, EdgeBundle],
    val inputVertexAttributes: Map[String, VertexAttribute[_]],
    val inputEdgeAttributes: Map[String, EdgeAttribute[_]]) extends Serializable {

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

  val rdd: VertexSetRDD
}

trait EdgeBundleData {
  val edgeBundle: EdgeBundle

  val rdd: EdgeBundleRDD
}

trait VertexAttributeData[+T] {
  val vertexSet: VertexSet

  val rdd: AttributeRDD[_ <: T]
}

trait EdgeAttributeData[T] {
  val edgeBundle: EdgeBundle

  val rdd: AttributeRDD[T]
}

trait DataManager {
  def obtainVertexSetData(vertexSet: VertexSet): VertexSetData
  def obtainEdgeBundleData(edgeBundle: EdgeBundle): EdgeBundleData
  def obtainVertexAttributeData[T](vertexAttribute: VertexAttribute[T]): VertexAttributeData[T]
  def obtainEdgeAttributeData[T](edgeAttribute: EdgeAttribute[T]): EdgeAttributeData[T]

  // Saves the given component's data to disk.
  def saveDataToDisk(component: MetaGraphComponent)

  // Returns information about the current running enviroment.
  // Typically used by operations to optimize their execution.
  def runtimeContext: RuntimeContext
}
