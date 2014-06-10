package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.spark.rdd
import scala.reflect.runtime.universe._
import scala.collection.mutable

import attributes.AttributeSignature

sealed trait MetaGraphEntity extends Serializable {
  val source: MetaGraphOperationInstance
  val name: String
  // Implement from source operation's GUID, name and the actual class of this component.
  val gUID: UUID = null
}

case class VertexSet(source: MetaGraphOperationInstance,
                     name: String) extends MetaGraphEntity

case class EdgeBundle(source: MetaGraphOperationInstance,
                      name: String) extends MetaGraphEntity {
  @transient lazy val (srcName, dstName) = source.operation.outputEdgeBundles(name)
  @transient lazy val srcVertexSet: VertexSet = source.entities.vertexSets(srcName)
  @transient lazy val dstVertexSet: VertexSet = source.entities.vertexSets(dstName)
  @transient lazy val isLocal = srcVertexSet == dstVertexSet
}

class Attribute[+T: TypeTag] {
  def typeTag: TypeTag[_ <: T] = implicitly[TypeTag[T]]
}

case class VertexAttribute[T: TypeTag](source: MetaGraphOperationInstance,
                                       name: String)
    extends Attribute with MetaGraphEntity {
  @transient lazy val vertexSet: VertexSet =
    source.entities.vertexSets(source.operation.outputVertexAttributes(name)._1)
}

case class EdgeAttribute[T: TypeTag](source: MetaGraphOperationInstance,
                                     name: String)
    extends Attribute with MetaGraphEntity {
  @transient lazy val edgeBundle: EdgeBundle =
    source.entities.edgeBundles(source.operation.outputEdgeAttributes(name)._1)
}

trait MetaGraphOperation extends Serializable {
  // Override "signature" to easily describe the inputs and outputs of your operation. E.g.:
  //     class MyOperation extends MetaGraphOperation {
  //       def signature = newSignature
  //         .inputGraph("input-vertices", "input-edges")
  //         .outputVertexAttribute[Double]("input-vertices", "my-attribute")
  //     }
  def signature: MetaGraphOperationSignature
  def newSignature = new MetaGraphOperationSignature

  // Names of vertex set inputs for this operation.
  def inputVertexSets: Set[String] = signature.inputVertexSets.toSet

  // Names of input bundles together with their source and destination vertex set names (which all
  // must be elements of inputVertexSets).
  def inputEdgeBundles: Map[String, (String, String)] = signature.inputEdgeBundles.toMap

  // Names of input vertex attributes together with their corresponding VertexSet names (which all
  // must be elements of inputVertexSets) and the types of the attributes.
  def inputVertexAttributes: Map[String, (String, TypeTag[_])] = signature.inputVertexAttributes.toMap

  // Names of input edge attributes together with their corresponding EdgeBundle names (which all
  // must be keys of inputEdgeBundles) and the types of the attributes.
  def inputEdgeAttributes: Map[String, (String, TypeTag[_])] = signature.inputEdgeAttributes.toMap

  // Names of vertex sets newly created by this operation.
  // (outputVertexSets /\ inputVertexSets needs to be empty).
  def outputVertexSets: Set[String] = signature.outputVertexSets.toSet

  // Names of bundles newly created by this operation together with their source and destination
  // vertex set names (which all must be elements of inputVertexSets \/ outputVertexSets).
  def outputEdgeBundles: Map[String, (String, String)] = signature.outputEdgeBundles.toMap

  // Names of vertex attributes newly created by this operation together with their corresponding
  // VertexSet names (which all must be elements of inputVertexSets \/ outputVertexSets)
  // and the types of the attributes.
  def outputVertexAttributes: Map[String, (String, TypeTag[_])] = signature.outputVertexAttributes.toMap

  // Names of edge attributes newly created by this operation together with their corresponding
  // EdgeBundle names (which all must be keys of inputEdgeBundles \/ outputEdgeBundles)
  // and the types of the attributes.
  def outputEdgeAttributes: Map[String, (String, TypeTag[_])] = signature.outputEdgeAttributes.toMap

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

  def execute(inputs: DataSet, outputs: DataSet, rc: RuntimeContext): Unit
}

class MetaGraphOperationSignature private[graph_api] {
  val inputVertexSets: mutable.Set[String] = mutable.Set()
  val inputEdgeBundles: mutable.Map[String, (String, String)] = mutable.Map()
  val inputVertexAttributes: mutable.Map[String, (String, TypeTag[_])] = mutable.Map()
  val inputEdgeAttributes: mutable.Map[String, (String, TypeTag[_])] = mutable.Map()
  val outputVertexSets: mutable.Set[String] = mutable.Set()
  val outputEdgeBundles: mutable.Map[String, (String, String)] = mutable.Map()
  val outputVertexAttributes: mutable.Map[String, (String, TypeTag[_])] = mutable.Map()
  val outputEdgeAttributes: mutable.Map[String, (String, TypeTag[_])] = mutable.Map()
  val allNames: mutable.Set[String] = mutable.Set()
  def inputVertexSet(name: String) = {
    assert(!allNames.contains(name), s"Double-defined: $name")
    inputVertexSets += name
    allNames += name
    this
  }
  def inputEdgeBundle(name: String, srcDst: (String, String)) = {
    assert(!allNames.contains(name), s"Double-defined: $name")
    inputEdgeBundles(name) = srcDst
    allNames += name
    this
  }
  def inputGraph(vertexSetName: String, edgeBundleName: String) = {
    inputVertexSet(vertexSetName)
    inputEdgeBundle(edgeBundleName, vertexSetName -> vertexSetName)
  }
  def inputVertexAttribute[T: TypeTag](vertexSetName: String, attributeName: String) = {
    assert(!allNames.contains(attributeName), s"Double-defined: $attributeName")
    inputVertexAttributes(attributeName) = vertexSetName -> typeTag[T]
    allNames += attributeName
    this
  }
  def inputEdgeAttribute[T: TypeTag](edgeBundleName: String, attributeName: String) = {
    assert(!allNames.contains(attributeName), s"Double-defined: $attributeName")
    inputEdgeAttributes(attributeName) = edgeBundleName -> typeTag[T]
    allNames += attributeName
    this
  }
  def outputVertexSet(name: String) = {
    assert(!allNames.contains(name), s"Double-defined: $name")
    outputVertexSets += name
    allNames += name
    this
  }
  def outputEdgeBundle(name: String, srcDst: (String, String)) = {
    assert(!allNames.contains(name), s"Double-defined: $name")
    outputEdgeBundles(name) = srcDst
    allNames += name
    this
  }
  def outputGraph(vertexSetName: String, edgeBundleName: String) = {
    outputVertexSet(vertexSetName)
    outputEdgeBundle(edgeBundleName, vertexSetName -> vertexSetName)
  }
  def outputVertexAttribute[T: TypeTag](vertexSetName: String, attributeName: String) = {
    assert(!allNames.contains(attributeName), s"Double-defined: $attributeName")
    outputVertexAttributes(attributeName) = vertexSetName -> typeTag[T]
    allNames += attributeName
    this
  }
  def outputEdgeAttribute[T: TypeTag](edgeBundleName: String, attributeName: String) = {
    assert(!allNames.contains(attributeName), s"Double-defined: $attributeName")
    outputEdgeAttributes(attributeName) = edgeBundleName -> typeTag[T]
    allNames += attributeName
    this
  }
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

  val entities = inputs ++ outputs
}

trait MetaGraphManager {
  def apply(operationInstance: MetaGraphOperationInstance): Unit
  def vertexSetForGUID(gUID: UUID): VertexSet
  def edgeBundleForGUID(gUID: UUID): EdgeBundle

  def incomingBundles(vertexSet: VertexSet): Seq[EdgeBundle]
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle]
  def dependentOperations(component: MetaGraphEntity): Seq[MetaGraphOperationInstance]
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
  def saveDataToDisk(component: MetaGraphEntity)

  // Returns information about the current running enviroment.
  // Typically used by operations to optimize their execution.
  def runtimeContext: RuntimeContext
}

// A bundle of metadata types.
case class MetaDataSet(vertexSets: Map[String, VertexSet] = Map(),
                       edgeBundles: Map[String, EdgeBundle] = Map(),
                       vertexAttributes: Map[String, VertexAttribute[_]] = Map(),
                       edgeAttributes: Map[String, EdgeAttribute[_]] = Map()) {
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

// A mutable bundle of data types.
case class DataSet(vertexSets: mutable.Map[String, VertexSetData] = mutable.Map(),
                   edgeBundles: mutable.Map[String, EdgeBundleData] = mutable.Map(),
                   vertexAttributes: mutable.Map[String, VertexAttributeData[_]] = mutable.Map(),
                   edgeAttributes: mutable.Map[String, EdgeAttributeData[_]] = mutable.Map(),
                   defaultInstance: Option[MetaGraphOperationInstance] = None) {
  def metaDataSet = MetaDataSet(
    vertexSets.mapValues(_.vertexSet).toMap,
    edgeBundles.mapValues(_.edgeBundle).toMap) // TODO: Attributes.

  def putVertexSet(name: String, rdd: VertexSetRDD) = {
    vertexSets(name) = new VertexSetData(defaultInstance.get.entities.vertexSets(name), rdd)
    this
  }
  def putEdgeBundle(name: String, rdd: EdgeBundleRDD) = {
    edgeBundles(name) = new EdgeBundleData(defaultInstance.get.entities.edgeBundles(name), rdd)
    this
  }
  def putVertexAttribute(name: String, rdd: AttributeRDD[_]) = {
    vertexAttributes(name) = new VertexAttributeData(defaultInstance.get.entities.vertexAttributes(name).vertexSet, rdd)
    this
  }
  def putEdgeAttribute(name: String, rdd: AttributeRDD[_]) = {
    edgeAttributes(name) = new EdgeAttributeData(defaultInstance.get.entities.edgeAttributes(name).edgeBundle, rdd)
    this
  }
}
