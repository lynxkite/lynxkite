package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.spark.rdd
import scala.reflect.runtime.universe._

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
  // Names of vertex set inputs for this operation.
  def inputVertexSets: Set[String] = inputs.flatMap {
    case Name.VS(name) => Some(name)
    case _ => None
  }.toSet

  // Names of input bundles together with their source and destination vertex set names (which all
  // must be elements of inputVertexSets).
  def inputEdgeBundles: Map[String, (String, String)] = inputs.flatMap {
    case Name.EB(name, srcDst) => Some(name -> srcDst)
    case _ => None
  }.toMap

  // Names of input vertex attributes together with their corresponding VertexSet names (which all
  // must be elements of inputVertexSets) and the types of the attributes.
  def inputVertexAttributes: Map[String, (String, TypeTag[_])] = inputs.filter(_.isInstanceOf[Name.VA]).map {
    e =>
      val va = e.asInstanceOf[Name.VA]
      va.bundleName -> (va.attrName, va.tt)
  }.toMap

  // Names of input edge attributes together with their corresponding EdgeBundle names (which all
  // must be keys of inputEdgeBundles) and the types of the attributes.
  def inputEdgeAttributes: Map[String, (String, TypeTag[_])] = inputs.filter(_.isInstanceOf[Name.EA]).map {
    e =>
      val ea = e.asInstanceOf[Name.EA]
      ea.bundleName -> (ea.attrName, ea.tt)
  }.toMap

  // Names of vertex sets newly created by this operation.
  // (outputVertexSets /\ inputVertexSets needs to be empty).
  def outputVertexSets: Set[String] = outputs.flatMap {
    case Name.VS(name) => Some(name)
    case _ => None
  }.toSet

  // Names of bundles newly created by this operation together with their source and destination
  // vertex set names (which all must be elements of inputVertexSets \/ outputVertexSets).
  def outputEdgeBundles: Map[String, (String, String)] = outputs.flatMap {
    case Name.EB(name, srcDst) => Some(name -> srcDst)
    case _ => None
  }.toMap

  // Names of vertex attributes newly created by this operation together with their corresponding
  // VertexSet names (which all must be elements of inputVertexSets \/ outputVertexSets)
  // and the types of the attributes.
  def outputVertexAttributes: Map[String, (String, TypeTag[_])] = outputs.filter(_.isInstanceOf[Name.VA]).map {
    e =>
      val va = e.asInstanceOf[Name.VA]
      va.bundleName -> (va.attrName, va.tt)
  }.toMap

  // Names of edge attributes newly created by this operation together with their corresponding
  // EdgeBundle names (which all must be keys of inputEdgeBundles \/ outputEdgeBundles)
  // and the types of the attributes.
  def outputEdgeAttributes: Map[String, (String, TypeTag[_])] = outputs.filter(_.isInstanceOf[Name.EA]).map {
    e =>
      val ea = e.asInstanceOf[Name.EA]
      ea.bundleName -> (ea.attrName, ea.tt)
  }.toMap

  // Use Name for quickly constructing inputs and outputs in subclasses. E.g.:
  //     class MyOp extends GraphOperation {
  //       override def inputs = (Name.vertexSet("graph-1") ++ Name.vertexSet("graph-2") ++
  //                              Name.EdgeBundle("input-edges", "graph-1" -> "graph-2"))
  //       override def outputs = Name.vertexAttribute[Double]("graph-1")("something")
  def inputs = Seq[Name.Entity]()
  def outputs = Seq[Name.Entity]()

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

object Name {
  def vertexSet(name: String): Seq[Entity] = Seq(VS(name))
  def edgeBundle(name: String, srcDst: (String, String)): Seq[Entity] = Seq(EB(name, srcDst))
  def edgeAttribute[T: TypeTag](bundleName: String)(attrName: String): Seq[Entity] =
    Seq(EA(bundleName, attrName, typeTag[T]))
  def vertexAttribute[T: TypeTag](bundleName: String)(attrName: String): Seq[Entity] =
    Seq(VA(bundleName, attrName, typeTag[T]))
  private[graph_api] trait Entity
  private[graph_api] case class VS(name: String) extends Entity
  private[graph_api] case class EB(name: String, srcDst: (String, String)) extends Entity
  import scala.language.existentials
  private[graph_api] case class VA(bundleName: String, attrName: String, tt: TypeTag[_]) extends Entity
  private[graph_api] case class EA(bundleName: String, attrName: String, tt: TypeTag[_]) extends Entity
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
case class DataSet(vertexSets: Map[String, VertexSetData] = Map(),
                   edgeBundles: Map[String, EdgeBundleData] = Map(),
                   vertexAttributes: Map[String, VertexAttributeData[_]] = Map(),
                   edgeAttributes: Map[String, EdgeAttributeData[_]] = Map())
object DataSet {
  def apply(inst: MetaGraphOperationInstance) = new DataSetBuilder(inst)
}

class DataSetBuilder(inst: MetaGraphOperationInstance,
                     vertexSets: Map[String, VertexSetData] = Map(),
                     edgeBundles: Map[String, EdgeBundleData] = Map(),
                     vertexAttributes: Map[String, VertexAttributeData[_]] = Map(),
                     edgeAttributes: Map[String, EdgeAttributeData[_]] = Map()) extends DataSet(vertexSets, edgeBundles, vertexAttributes, edgeAttributes) {
  def vertexSet(name: String, rdd: VertexSetRDD) =
    new DataSetBuilder(
      inst,
      vertexSets + (name -> new VertexSetData(inst.entities.vertexSets(name), rdd)),
      edgeBundles,
      vertexAttributes,
      edgeAttributes)

  def edgeBundle(name: String, rdd: EdgeBundleRDD) =
    new DataSetBuilder(
      inst,
      vertexSets,
      edgeBundles + (name -> new EdgeBundleData(inst.entities.edgeBundles(name), rdd)),
      vertexAttributes,
      edgeAttributes)

  def vertexAttribute(name: String, rdd: AttributeRDD[_]) =
    new DataSetBuilder(
      inst,
      vertexSets,
      edgeBundles,
      vertexAttributes + (name -> new VertexAttributeData(
        inst.entities.vertexAttributes(name).vertexSet, rdd)),
      edgeAttributes)

  def edgeAttribute(name: String, rdd: AttributeRDD[_]) =
    new DataSetBuilder(
      inst,
      vertexSets,
      edgeBundles,
      vertexAttributes,
      edgeAttributes + (name -> new EdgeAttributeData(
        inst.entities.edgeAttributes(name).edgeBundle, rdd)))
}
