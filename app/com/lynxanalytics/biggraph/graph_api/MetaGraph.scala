package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.spark.rdd
import scala.reflect.runtime.universe._

import attributes.AttributeSignature

sealed trait MetaGraphComponent extends Serializable {
  val sourceOperation: MetaGraphOperationInstance
  val name: String
  // Implement from source operation's GUID, name and the actual class of this component.
  val gUID: UUID = null
}

case class VertexSet(sourceOperation: MetaGraphOperationInstance,
                     name: String) extends MetaGraphComponent

case class EdgeBundle(sourceOperation: MetaGraphOperationInstance,
                      name: String) extends MetaGraphComponent {
  @transient lazy val (srcName, dstName) = sourceOperation.operation.outputEdgeBundles(name)
  @transient lazy val srcVertexSet: VertexSet = sourceOperation.components.vertexSets(srcName)
  @transient lazy val dstVertexSet: VertexSet = sourceOperation.components.vertexSets(dstName)
  @transient lazy val isLocal = srcVertexSet == dstVertexSet
}

class Attribute[+T: TypeTag] {
  def typeTag: TypeTag[_ <: T] = implicitly[TypeTag[T]]
}

case class VertexAttribute[T: TypeTag](sourceOperation: MetaGraphOperationInstance,
                                       name: String)
    extends Attribute with MetaGraphComponent {
  @transient lazy val vertexSet: VertexSet =
    sourceOperation.components.vertexSets(sourceOperation.operation.outputVertexAttributes(name)._1)
}

case class EdgeAttribute[T: TypeTag](sourceOperation: MetaGraphOperationInstance,
                                     name: String)
    extends Attribute with MetaGraphComponent {
  @transient lazy val edgeBundle: EdgeBundle =
    sourceOperation.components.edgeBundles(sourceOperation.operation.outputEdgeAttributes(name)._1)
}

trait MetaGraphOperation extends Serializable {
  // Names of vertex set inputs for this operation.
  def inputVertexSets: Set[String] = Set()

  // Names of input bundles together with their source and desination vertex set names (which all
  // must be elements of inputVertexSetNames).
  def inputEdgeBundles: Map[String, (String, String)] = Map()

  // Names of input vertex attributes together with their corresponding VertexSet names (which all
  // must be elements of inputVertexSetNames) and the types of the attributes.
  def inputVertexAttributes: Map[String, (String, TypeTag[_])] = Map()

  // Names of input edge attributes together with their corresponding EdgeBundle names (which all
  // must be keys of inputEdgeBundleDefs) and the types of the attributes.
  def inputEdgeAttributes: Map[String, (String, TypeTag[_])] = Map()

  // Names of vertex sets newly created by this operation.
  // (outputVertexSetNamess /\ inputVertexSetNames needs to be empty).
  def outputVertexSets: Set[String] = Set()

  // Names of bundles newly created by this operation together with their source and desination
  // vertex set names (which all must be elements of inputVertexSetNames \/ outputVertexSetNames).
  def outputEdgeBundles: Map[String, (String, String)] = Map()

  // Names of vertex attributes newly created by this operation together with their corresponding
  // VertexSet names (which all must be elements of inputVertexSetNames \/ outputVertexSetNames)
  // and the types of the attributes.
  def outputVertexAttributes: Map[String, (String, TypeTag[_])] = Map()

  // Names of edge attributes newly created by this operation together with their corresponding
  // EdgeBundle names (which all must be keys of inputEdgeBundleDefs \/ outputEdgeBundleDefs)
  // and the types of the attributes.
  def outputEdgeAttributes: Map[String, (String, TypeTag[_])] = Map()

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

  def execute(inst: MetaGraphOperationInstance, dataManager: DataManager): DataSet
}

/*
 * Base class for concrete instances of MetaGraphOperations. An instance of an operation is
 * the operation together with concrete input vertex sets and edge bundles.
 */
case class MetaGraphOperationInstance(
    val operation: MetaGraphOperation,
    val inputs: MetaDataSet) {

  val gUID: UUID = null // TODO implement here from guids of inputs and operation.

  val outputs = MetaDataSet(
    operation.outputVertexSets.map(n => n -> VertexSet(this, n)).toMap,
    operation.outputEdgeBundles.keys.map(n => n -> EdgeBundle(this, n)).toMap,
    operation.outputVertexAttributes.map {
      case (n, (vs, tt)) => n -> VertexAttribute(this, n)(tt)
    }.toMap,
    operation.outputEdgeAttributes.map {
      case (n, (vs, tt)) => n -> EdgeAttribute(this, n)(tt)
    }.toMap)

  val components = inputs ++ outputs
}

trait MetaGraphManager {
  def apply(operationInstance: MetaGraphOperationInstance): Unit
  def vertexSetForGUID(gUID: UUID): VertexSet
  def edgeBundleForGUID(gUID: UUID): EdgeBundle

  def incomingBundles(vertexSet: VertexSet): Seq[EdgeBundle]
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle]
  def dependentOperations(component: MetaGraphComponent): Seq[MetaGraphOperationInstance]
}

class VertexSetData(val vertexSet: VertexSet,
                    val rdd: VertexSetRDD)

class EdgeBundleData(val edgeBundle: EdgeBundle,
                     val rdd: EdgeBundleRDD)

class VertexAttributeData[T](val vertexSet: VertexSet,
                             val rdd: AttributeRDD[T])

class EdgeAttributeData[T](val edgeBundle: EdgeBundle,
                           val rdd: AttributeRDD[T])

trait DataManager {
  def get(vertexSet: VertexSet): VertexSetData
  def get(edgeBundle: EdgeBundle): EdgeBundleData
  def get[T](vertexAttribute: VertexAttribute[T]): VertexAttributeData[T]
  def get[T](edgeAttribute: EdgeAttribute[T]): EdgeAttributeData[T]

  // Saves the given component's data to disk.
  def saveDataToDisk(component: MetaGraphComponent)

  // Returns information about the current running enviroment.
  // Typically used by operations to optimize their execution.
  def runtimeContext: RuntimeContext
}

// A bundle of metadata types.
case class MetaDataSet(val vertexSets: Map[String, VertexSet] = Map(),
                       val edgeBundles: Map[String, EdgeBundle] = Map(),
                       val vertexAttributes: Map[String, VertexAttribute[_]] = Map(),
                       val edgeAttributes: Map[String, EdgeAttribute[_]] = Map()) {
  def ++(mds: MetaDataSet): MetaDataSet = {
    assert((vertexSets.keySet & mds.vertexSets.keySet).isEmpty,
      "Collision: " + (vertexSets.keySet & mds.vertexSets.keySet).toSeq)
    assert((edgeBundles.keySet & mds.edgeBundles.keySet).isEmpty,
      "Collision: " + (edgeBundles.keySet & mds.edgeBundles.keySet).toSeq)
    assert((vertexAttributes.keySet & mds.vertexAttributes.keySet).isEmpty,
      "Collision: " + (vertexAttributes.keySet & mds.vertexAttributes.keySet).toSeq)
    assert((edgeAttributes.keySet & mds.edgeAttributes.keySet).isEmpty,
      "Collision: " + (edgeAttributes.keySet & mds.edgeAttributes.keySet).toSeq)
    return MetaDataSet(
      vertexSets ++ mds.vertexSets,
      edgeBundles ++ mds.edgeBundles,
      vertexAttributes ++ mds.vertexAttributes,
      edgeAttributes ++ mds.edgeAttributes)
  }
}

// A bundle of data types.
case class DataSet(val vertexSets: Map[String, VertexSetData] = Map(),
                   val edgeBundles: Map[String, EdgeBundleData] = Map(),
                   val vertexAttributes: Map[String, VertexAttributeData[_]] = Map(),
                   val edgeAttributes: Map[String, EdgeAttributeData[_]] = Map())
object DataSet {
  // Convenience constructor.
  def forInstance(inst: MetaGraphOperationInstance)(
    vertexSets: Map[String, VertexSetRDD] = Map(),
    edgeBundles: Map[String, EdgeBundleRDD] = Map(),
    vertexAttributes: Map[String, AttributeRDD[_]] = Map(),
    edgeAttributes: Map[String, AttributeRDD[_]] = Map()) = {
    new DataSet(
      vertexSets.map {
        case (name, rdd) =>
          name -> new VertexSetData(inst.components.vertexSets(name), rdd)
      }, edgeBundles.map {
        case (name, rdd) =>
          name -> new EdgeBundleData(inst.components.edgeBundles(name), rdd)
      }, vertexAttributes.map {
        case (name, rdd) =>
          val attr = inst.components.vertexAttributes(name)
          name -> new VertexAttributeData(attr.vertexSet, rdd)
      }.toMap, edgeAttributes.map {
        case (name, rdd) =>
          val attr = inst.components.edgeAttributes(name)
          name -> new EdgeAttributeData(attr.edgeBundle, rdd)
      }.toMap)
  }
}
