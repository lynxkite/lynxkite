// The core classes for representing our data.
//
// The "metagraph" is made up of MetaGraphEntities (vertex sets, edge bundles,
// attributes, scalars, and tables) and MetaGraphOperationInstances. The entities are
// the inputs and outputs of the operation instances. The operation instance is
// an operation that is bound to a set of inputs.
//
// On this "meta" level, there is no data. It is just about identifying the entities.
// The entities are identified by UUIDs in storage. These UUIDs (or GUIDs) are a hash of
// the whole tree of the metagraph leading up to the entity. This is how we track what
// needs to be recomputed when something changes. Everything will get a different UUID
// downstream from the change, and those UUIDs have no corresponding data yet.
//
// This file also includes the machinery for declaring operation input/output
// signatures.

package com.lynxanalytics.biggraph.graph_api

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.util.IdentityHashMap
import java.util.UUID
import scala.reflect.runtime.universe._
import scala.Symbol // There is a Symbol in the universe package too.
import scala.collection.mutable
import scala.collection.immutable.SortedMap
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import org.apache.spark

sealed trait MetaGraphEntity extends Serializable {
  val source: MetaGraphOperationInstance
  val name: Symbol
  override def toString = s"$gUID (${name.name} of $source)"
  def manager = source.manager
  lazy val typeString = this.getClass.getSimpleName

  // An entity is identified by the operation that produced it, which output of the operation it is,
  // and what kind of entity it is.
  lazy val gUID: UUID = {
    val buffer = new ByteArrayOutputStream
    val objectStream = new ObjectOutputStream(buffer)
    objectStream.writeObject(name)
    objectStream.writeObject(source.gUID)
    objectStream.writeObject(this.getClass.toString)
    objectStream.close()
    val a = buffer.toByteArray
    // Symbol's serialized form changed from Scala 2.11 to Scala 2.12.
    // To avoid new GUIDs for every entity when upgrading from LynxKite 4.2,
    // we patch the byte array to the old form. This is never deserialized anyway.
    assert(
      a.slice(0, newPrefix.length).toList == newPrefix,
      s"Unexpected serialized form for $this: ${a.map(x => "%x ".format(x.toInt)).mkString}")
    for (i <- 0 until newPrefix.length) {
      a(i) = oldPrefix(i)
    }
    UUID.nameUUIDFromBytes(a)
  }
  private val oldPrefix = "ACED00057372000C7363616C612E53796D626F6C292AC645426F2F4B"
    .sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toSeq
  private val newPrefix = "ACED00057372000C7363616C612E53796D626F6C5F4785C13785FF06"
    .sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toSeq
}

case class VertexSet(
    source: MetaGraphOperationInstance,
    name: Symbol)
    extends MetaGraphEntity {
  assert(name != null, s"name is null for $this")
}

/*
 * Represents potential extra properties of edge bundles.
 *
 * This class can be used both to represent properties of a bundle or requirements against
 * a bundle by an operation. In either case, only "true" values matter. E.g. isFunction = false
 * does not mean that the bundle is definitely not a function, it only means that we are not sure
 * about its functionness.
 */
case class EdgeBundleProperties(
    // If you add a new property don't forget to update methods below as well!

    // The edge bundle defines a (potentially partial) function from its source
    // to its destination. Equivalently, all source vertices have an outdegree <= 1.
    isFunction: Boolean = false,
    // The edge bundle defines a (potentially partial) function from its destination
    // to its source. Equivalently, all destination vertices have an indegree <= 1.
    isReversedFunction: Boolean = false,
    // All source vertices have at least one outgoing edge.
    isEverywhereDefined: Boolean = false,
    // All destination vertices have at least one incoming edge.
    isReverseEverywhereDefined: Boolean = false,
    // The source id and destination id are the same for all edges in this bundle.
    // In this case edge ids are also chosen to match the source and destination ids.
    isIdPreserving: Boolean = false) {

  override def toString: String = {
    ((if (isFunction) Some("function") else None) ::
      (if (isReversedFunction) Some("reversed-function") else None) ::
      (if (isEverywhereDefined) Some("everywhere-defined") else None) ::
      (if (isReverseEverywhereDefined) Some("reverse-everywhere-defined") else None) ::
      (if (isIdPreserving) Some("id-preserving") else None) ::
      Nil).flatten.mkString(" ")
  }

  def compliesWith(requirements: EdgeBundleProperties): Boolean =
    (isFunction || !requirements.isFunction) &&
      (isReversedFunction || !requirements.isReversedFunction) &&
      (isEverywhereDefined || !requirements.isEverywhereDefined) &&
      (isReverseEverywhereDefined || !requirements.isReverseEverywhereDefined) &&
      (isIdPreserving || !requirements.isIdPreserving)

  lazy val reversed: EdgeBundleProperties =
    EdgeBundleProperties(
      isReversedFunction,
      isFunction,
      isReverseEverywhereDefined,
      isEverywhereDefined,
      isIdPreserving)
}
object EdgeBundleProperties {
  val default = EdgeBundleProperties()
  val partialFunction = EdgeBundleProperties(isFunction = true)
  val matching = EdgeBundleProperties(isFunction = true, isReversedFunction = true)
  val injection = matching.copy(isEverywhereDefined = true)
  val bijection = injection.copy(isReverseEverywhereDefined = true)
  val embedding = injection.copy(isIdPreserving = true)
  val identity = bijection.copy(isIdPreserving = true)
  val surjection = partialFunction.copy(isReverseEverywhereDefined = true)
}

case class EdgeBundle(
    source: MetaGraphOperationInstance,
    name: Symbol,
    srcVertexSet: VertexSet,
    dstVertexSet: VertexSet,
    properties: EdgeBundleProperties = EdgeBundleProperties.default,
    idSet: VertexSet, // The edge IDs as a VertexSet.
    autogenerateIdSet: Boolean) // The RDD for idSet will be auto-generated.
    extends MetaGraphEntity {
  assert(name != null, s"name is null for $this")
  val isLocal = srcVertexSet == dstVertexSet
}

// A MetaGraph entity created from the src->dst mappings of an EdgeBundle.
case class HybridBundle(
    source: MetaGraphOperationInstance,
    name: Symbol,
    // Always uses the src->dst mapping, the edge IDs are ignored.
    srcToDstEdgeBundle: EdgeBundle)
    extends MetaGraphEntity {
  assert(name != null, s"name is null for $this")
}

sealed trait TypedEntity[T] extends MetaGraphEntity {
  val typeTag: TypeTag[T]
  def runtimeSafeCast[S: TypeTag]: TypedEntity[S]
  def is[S: TypeTag]: Boolean
}

case class Attribute[T: TypeTag](
    source: MetaGraphOperationInstance,
    name: Symbol,
    vertexSet: VertexSet)
    extends TypedEntity[T] with RuntimeSafeCastable[T, Attribute] {
  assert(name != null, s"name is null for $this")
  val typeTag = implicitly[TypeTag[T]]
}

case class Scalar[T: TypeTag](
    source: MetaGraphOperationInstance,
    name: Symbol)
    extends TypedEntity[T] with RuntimeSafeCastable[T, Scalar] {
  assert(name != null, s"name is null for $this")
  val typeTag = implicitly[TypeTag[T]]
}

case class Table(
    source: MetaGraphOperationInstance,
    name: Symbol,
    schema: spark.sql.types.StructType)
    extends MetaGraphEntity {
  assert(name != null, s"name is null for $this")
}

case class InputSignature(
    vertexSets: Set[Symbol],
    edgeBundles: Set[Symbol],
    hybridBundles: Set[Symbol],
    attributes: Set[Symbol],
    scalars: Set[Symbol],
    tables: Set[Symbol])
trait InputSignatureProvider {
  def inputSignature: InputSignature
}

// Using MagicInputSignature we can write "val vs = vertexSet" and it declares an input called "vs".
// The trait below solves the problem of figuring out that "input.vs" has the name "vs".
trait FieldNaming {
  private lazy val naming: IdentityHashMap[Any, Symbol] = {
    val res = new IdentityHashMap[Any, Symbol]()
    val mirror = reflect.runtime.currentMirror.reflect(this)

    val fields = mirror.symbol.toType.members.collect {
      case m: MethodSymbol if (m.isGetter && m.isPublic && !m.isLazy) => m
    }
    for (m <- fields) {
      res.put(mirror.reflectField(m).get, Symbol(m.name.toString))
    }
    res
  }
  // Returns the name of the field in this object that refers to the given object.
  def nameOf(obj: Any): Symbol = {
    val name = naming.get(obj)
    assert(
      name != null,
      "This is typically caused by a name being used before the initialization of " +
        "the FieldNaming subclass. We were looking for the name of: %s. Available names: %s".format(
          obj,
          naming),
    )
    name
  }
}

// The input declarations in a MagicInputSignature are instances of EntityTemplate.
// This class is not usually used directly. An implicit conversion to the actual
// MetaGraphEntity is usually used to work with the actual input entity of an operation.
trait EntityTemplate[T <: MetaGraphEntity] {
  def set(target: MetaDataSet, entity: T): MetaDataSet
  def entity(implicit instance: MetaGraphOperationInstance): T
  val name: Symbol
}
object EntityTemplate {
  import scala.language.implicitConversions
  implicit def unpackTemplate[T <: MetaGraphEntity](
      template: EntityTemplate[T])(
      implicit instance: MetaGraphOperationInstance): T = template.entity
}

// MagicInputSignature makes it easy to declare the inputs of a MetaGraphOp. More importantly,
// it enables compile-time checks for users of the operation.
//
// An example declaration:
//
//   class Input extends MagicInputSignature {
//     val input1 = vertexSet
//     val input2 = attribute[Double](input1)
//   }
//
// An operation with this input signature can then be invoked as:
//
//   metaGraphManager.apply(op, ('input1 -> vs), ('input2 -> attr))
//
// But this syntax does not protect against typos in the input name or type mistakes.
// (Such as swapping "vs" and "attr" in the example.) This is solved by the mechanism in
// Scripting.scala in this directory. The example continues there!
abstract class MagicInputSignature extends InputSignatureProvider with FieldNaming {
  abstract class ET[T <: MetaGraphEntity](nameOpt: Option[Symbol]) extends EntityTemplate[T] {
    lazy val name: Symbol = nameOpt.getOrElse(nameOf(this))
    def set(target: MetaDataSet, entity: T): MetaDataSet =
      MetaDataSet(Map(name -> entity)) ++ target
    def get(set: MetaDataSet): T = set.all(name).asInstanceOf[T]
    def entity(implicit instance: MetaGraphOperationInstance): T =
      get(instance.inputs)
    def meta(implicit dataSet: DataSet) = dataSet.all(name).entity.asInstanceOf[T]
    templates += this
  }

  class VertexSetTemplate(nameOpt: Option[Symbol]) extends ET[VertexSet](nameOpt) {
    def data(implicit dataSet: DataSet) = dataSet.vertexSets(name)
    def rdd(implicit dataSet: DataSet) = data.rdd
  }

  class EdgeBundleTemplate(
      srcF: => Symbol,
      dstF: => Symbol,
      idSetF: => Option[Symbol],
      requiredProperties: EdgeBundleProperties,
      nameOpt: Option[Symbol])
      extends ET[EdgeBundle](nameOpt) {
    private lazy val src = srcF
    private lazy val dst = dstF
    private lazy val idSet = idSetF
    override def set(target: MetaDataSet, eb: EdgeBundle): MetaDataSet = {
      assert(Option(eb).nonEmpty, "The project has no edge bundle")
      assert(
        eb.properties.compliesWith(requiredProperties),
        s"Edge bundle $eb (${eb.properties}) does not comply with: $requiredProperties")
      val withSrc =
        templatesByName(src).asInstanceOf[VertexSetTemplate].set(target, eb.srcVertexSet)
      val withSrcDst =
        templatesByName(dst).asInstanceOf[VertexSetTemplate].set(withSrc, eb.dstVertexSet)
      val withSrcDstIdSet = idSet match {
        case Some(vsName) => templatesByName(vsName).asInstanceOf[VertexSetTemplate]
            .set(withSrcDst, eb.idSet)
        case None => withSrcDst
      }
      super.set(withSrcDstIdSet, eb)
    }
    def data(implicit dataSet: DataSet) = dataSet.edgeBundles(name)
    def rdd(implicit dataSet: DataSet) = data.rdd
  }

  class HybridBundleTemplate(
      esF: => Symbol,
      nameOpt: Option[Symbol])
      extends ET[HybridBundle](nameOpt) {
    private lazy val es = esF
    override def set(target: MetaDataSet, hb: HybridBundle): MetaDataSet = {
      val withEs =
        templatesByName(es).asInstanceOf[EdgeBundleTemplate].set(target, hb.srcToDstEdgeBundle)
      super.set(withEs, hb)
    }
    def data(implicit dataSet: DataSet) = dataSet.hybridBundles(name)
    def rdd(implicit dataSet: DataSet) = data.rdd
  }

  class VertexAttributeTemplate[T](vsF: => Symbol, nameOpt: Option[Symbol])
      extends ET[Attribute[T]](nameOpt) {
    lazy val vs = vsF
    override def set(target: MetaDataSet, va: Attribute[T]): MetaDataSet = {
      val withVs =
        templatesByName(vs).asInstanceOf[VertexSetTemplate].set(target, va.vertexSet)
      super.set(withVs, va)
    }
    def data(implicit dataSet: DataSet) = dataSet.attributes(name).asInstanceOf[AttributeData[T]]
    def rdd(implicit dataSet: DataSet) = data.rdd
  }

  class RuntimeTypedVATemplate(vsF: => Symbol, nameOpt: Option[Symbol], tt: TypeTag[_])
      extends ET[Attribute[_]](nameOpt) {
    lazy val vs = vsF
    override def set(target: MetaDataSet, va: Attribute[_]): MetaDataSet = {
      assert(
        va.typeTag.tpe =:= tt.tpe,
        s"Attribute type ${va.typeTag.tpe} does not match required type ${tt.tpe} for attribute '${name}'.")
      val withVs =
        templatesByName(vs).asInstanceOf[VertexSetTemplate].set(target, va.vertexSet)
      super.set(withVs, va)
    }
    def data(implicit dataSet: DataSet) = dataSet.attributes(name)
    def rdd(implicit dataSet: DataSet) = data.rdd
  }

  class EdgeAttributeTemplate[T](esF: => Symbol, nameOpt: Option[Symbol])
      extends ET[Attribute[T]](nameOpt) {
    lazy val es = esF
    override def set(target: MetaDataSet, ea: Attribute[T]): MetaDataSet = {
      assert(
        target.edgeBundles.contains(es),
        s"The edge bundle input ($es) has to be provided before the attribute ($name).")
      val eb = templatesByName(es).asInstanceOf[EdgeBundleTemplate].get(target)
      assert(
        eb.idSet == ea.vertexSet,
        s"$name = $ea is for ${ea.vertexSet}, not for ${eb.idSet}")
      super.set(target, ea)
    }
    def data(implicit dataSet: DataSet) = dataSet.attributes(name).asInstanceOf[AttributeData[T]]
    def rdd(implicit dataSet: DataSet) = data.rdd
  }

  class ScalarTemplate[T](nameOpt: Option[Symbol]) extends ET[Scalar[T]](nameOpt) {
    def data(implicit dataSet: DataSet) = dataSet.scalars(name).asInstanceOf[ScalarData[T]]
    def value(implicit dataSet: DataSet) = data.value
  }

  class RuntimeTypedScalarTemplate(nameOpt: Option[Symbol], tt: TypeTag[_]) extends ET[Scalar[_]](nameOpt) {
    override def set(target: MetaDataSet, sc: Scalar[_]): MetaDataSet = {
      assert(
        sc.typeTag.tpe =:= tt.tpe,
        s"Scalar type ${sc.typeTag.tpe} does not match required type ${tt.tpe}.")
      super.set(target, sc)
    }
    def data(implicit dataSet: DataSet) = dataSet.scalars(name)
    def value(implicit dataSet: DataSet) = data.value
  }

  class TableTemplate(nameOpt: Option[Symbol]) extends ET[Table](nameOpt) {
    def data(implicit dataSet: DataSet) = dataSet.tables(name).asInstanceOf[TableData]
    def df(implicit dataSet: DataSet) = data.df
  }

  // Everything above is the machinery to make the methods below work.
  // Use these when declaring the inputs of your operation:
  def vertexSet = new VertexSetTemplate(None)
  def vertexSet(name: Symbol) = new VertexSetTemplate(Some(name))
  def edgeBundle(
      src: VertexSetTemplate,
      dst: VertexSetTemplate,
      requiredProperties: EdgeBundleProperties = EdgeBundleProperties.default,
      idSet: VertexSetTemplate = null,
      name: Symbol = null) =
    new EdgeBundleTemplate(
      src.name,
      dst.name,
      Option(idSet).map(_.name),
      requiredProperties,
      Option(name))
  def hybridBundle(
      es: EdgeBundleTemplate,
      name: Symbol = null) =
    new HybridBundleTemplate(
      es.name,
      Option(name))
  def vertexAttribute[T](vs: VertexSetTemplate, name: Symbol = null) =
    new VertexAttributeTemplate[T](vs.name, Option(name))
  def runtimeTypedVertexAttribute(vs: VertexSetTemplate, name: Symbol = null, tt: TypeTag[_]) =
    new RuntimeTypedVATemplate(vs.name, Option(name), tt)
  def edgeAttribute[T](es: EdgeBundleTemplate, name: Symbol = null) =
    new EdgeAttributeTemplate[T](es.name, Option(name))
  def scalar[T] = new ScalarTemplate[T](None)
  def scalar[T](name: Symbol) = new ScalarTemplate[T](Some(name))
  def runtimeTypedScalar(name: Symbol, tt: TypeTag[_]) =
    new RuntimeTypedScalarTemplate(Some(name), tt)
  def table = new TableTemplate(None)
  def table(name: Symbol) = new TableTemplate(Some(name))
  def graph = {
    val vs = vertexSet
    (vs, edgeBundle(vs, vs))
  }

  lazy val inputSignature: InputSignature =
    InputSignature(
      vertexSets = templates.collect { case vs: VertexSetTemplate => vs.name }.toSet,
      edgeBundles = templates.collect { case eb: EdgeBundleTemplate => eb.name }.toSet,
      hybridBundles = templates.collect { case hb: HybridBundleTemplate => hb.name }.toSet,
      attributes = templates.collect {
        case a: VertexAttributeTemplate[_] => a.name
        case a: EdgeAttributeTemplate[_] => a.name
        case a: RuntimeTypedVATemplate => a.name
      }.toSet,
      scalars = templates.collect {
        case sc: ScalarTemplate[_] => sc.name
        case sc: RuntimeTypedScalarTemplate => sc.name
      }.toSet,
      tables = templates.collect { case tb: TableTemplate => tb.name }.toSet,
    )

  private val templates = mutable.Buffer[ET[_ <: MetaGraphEntity]]()
  private lazy val templatesByName = {
    val pairs: Iterable[(Symbol, ET[_ <: MetaGraphEntity])] =
      templates.map(t => (t.name, t))
    pairs.toMap
  }
}

// Something that can provide a MetaDataSet. In practice this is used for MagicOutput below.
trait MetaDataSetProvider {
  def metaDataSet: MetaDataSet
}

// The placeholders in MagicOutput are EntityContainers. The companion object provides implicit
// conversion to MetaGraphEntity, so it can be used as an entity most of the time.
trait EntityContainer[+T <: MetaGraphEntity] {
  def entity: T
}
object EntityContainer {
  implicit class TrivialContainer[T <: MetaGraphEntity](val entity: T) extends EntityContainer[T]
  import scala.language.implicitConversions
  implicit def unpackContainer[T <: MetaGraphEntity](container: EntityContainer[T]): T =
    container.entity
}

// MagicOutput provides a nice syntax for declaring MetaGraphOp outputs and also for accessing
// those outputs from an instance of the operation.
//
// For example:
//
//   class Output(instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
//     val output1 = vertexSet
//     val output2 = attribute[Double](output1)
//   }
//
// Note the symmetry with MagicInputSignature.
//
// The outputs from a TypedOperationInstance can then be accessed as "op.result.output1".
// (Which is actually an EntityContainer, but it's automatically converted to a MetaGraphEntity
// when used as one.)
abstract class MagicOutput(instance: MetaGraphOperationInstance)
    extends MetaDataSetProvider with FieldNaming {
  class P[T <: MetaGraphEntity](entityConstructor: Symbol => T, nameOpt: Option[Symbol]) extends EntityContainer[T] {
    lazy val name: Symbol = {
      val name = nameOpt.getOrElse(nameOf(this))
      assert(
        !name.name.endsWith("-idSet"),
        "Output names ending with '-idSet' are reserved for automatically" +
          s" generated edge bundle id sets. Rejecting $name.")
      name
    }
    lazy val entity = entityConstructor(name)

    override def toString: String = {
      val fakeEntity = entityConstructor(nameOpt.getOrElse('fakeName))
      "P wrapper of type %s for %s".format(fakeEntity.getClass.getName, fakeEntity.toString)
    }

    placeholders += this
  }
  def vertexSet = new P(VertexSet(instance, _), None)
  def vertexSet(name: Symbol) = new P(VertexSet(instance, _), Some(name))
  def edgeBundle(
      src: => EntityContainer[VertexSet],
      dst: => EntityContainer[VertexSet],
      properties: EdgeBundleProperties = EdgeBundleProperties.default,
      idSet: EntityContainer[VertexSet] = null,
      name: Symbol = null) = {
    // A "var" is used because the edge bundle and its idSet need each other's references.
    var eb: P[EdgeBundle] = null
    val idSetSafe = Option(idSet).getOrElse {
      // If an idSet was not explicitly set, generate an output for it.
      new P(VertexSet(instance, _), None) {
        override lazy val name: Symbol = {
          Symbol(eb.name.name + "-idSet")
        }
      }
    }
    eb = new P(
      EdgeBundle(
        instance,
        _,
        src,
        dst,
        properties,
        idSetSafe,
        autogenerateIdSet = idSet == null),
      Option(name))
    eb
  }
  def hybridBundle(
      es: => EntityContainer[EdgeBundle],
      name: Symbol = null) = new P(HybridBundle(instance, _, es), Option(name))
  def graph = {
    val v = vertexSet
    (v, edgeBundle(v, v))
  }
  def vertexAttribute[T: TypeTag](vs: => EntityContainer[VertexSet], name: Symbol = null) =
    new P(Attribute[T](instance, _, vs), Option(name))
  def edgeAttribute[T: TypeTag](eb: => EntityContainer[EdgeBundle], name: Symbol = null) =
    new P(Attribute[T](instance, _, eb.idSet), Option(name))
  def scalar[T: TypeTag] = new P(Scalar[T](instance, _), None)
  def scalar[T: TypeTag](name: Symbol) = new P(Scalar[T](instance, _), Some(name))
  def table(schema: spark.sql.types.StructType, name: Symbol = null) =
    new P(Table(instance, _, schema), Option(name))

  private val placeholders = mutable.Buffer[P[_ <: MetaGraphEntity]]()

  lazy val metaDataSet = MetaDataSet(placeholders.map(_.entity).map(e => (e.name, e)).toMap)
}

object MetaGraphOp {
  val UTF8 = java.nio.charset.Charset.forName("UTF-8")
}
// An operation that has its parameters already set, but the inputs are not yet specified.
trait MetaGraphOp extends Serializable with ToJson {
  def inputSig: InputSignature
  def outputMeta(instance: MetaGraphOperationInstance): MetaDataSetProvider

  // An operation is identified by its class, parameters, and registered version
  // number, if any.
  val gUID = {
    val contents = play.api.libs.json.jackson.RetroSerialization(this.toTypedJson)
    val version = JsonMigration.current.version(getClass.getName)
    UUID.nameUUIDFromBytes((contents + version).getBytes(MetaGraphOp.UTF8))
  }
}

object TypedMetaGraphOp {
  // A little "hint" for the type inference.
  type Type = TypedMetaGraphOp[_ <: InputSignatureProvider, _ <: MetaDataSetProvider]
}
// An operation that can provide declarations of its inputs and outputs.
trait TypedMetaGraphOp[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider]
    extends MetaGraphOp {
  def inputs: IS = ???
  def inputSig: InputSignature = inputs.inputSignature
  def outputMeta(instance: MetaGraphOperationInstance): OMDS
}

/*
 * Base class for concrete instances of MetaGraphOperations. An instance of an operation is
 * the operation together with concrete input vertex sets and edge bundles.
 */
trait MetaGraphOperationInstance {
  val manager: MetaGraphManager

  val operation: MetaGraphOp

  val inputs: MetaDataSet

  // An operation instance is identified by the operation and all the inputs.
  val gUID: UUID = {
    val buffer = new ByteArrayOutputStream
    val objectStream = new ObjectOutputStream(buffer)
    objectStream.writeObject(operation.gUID)
    val names = inputs.all.keys.toSeq.map(_ match { case Symbol(s) => s }).sorted
    for (name <- names) {
      objectStream.writeObject(name)
      objectStream.writeObject(inputs.all(Symbol(name)).gUID)
    }
    objectStream.close()
    UUID.nameUUIDFromBytes(buffer.toByteArray)
  }

  val outputs: MetaDataSet

  def entities: MetaDataSet = inputs ++ outputs

  override def toString = s"$gUID ($operation)"
}

case class TypedOperationInstance[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    manager: MetaGraphManager,
    operation: TypedMetaGraphOp[IS, OMDS],
    inputs: MetaDataSet)
    extends MetaGraphOperationInstance {
  val result: OMDS = operation.outputMeta(this)
  val outputs: MetaDataSet = result.metaDataSet
  override lazy val hashCode = gUID.hashCode
}

// A bundle of named MetaGraphEntities, such as in an operation input or output.
case class MetaDataSet(
    vertexSets: Map[Symbol, VertexSet] = Map(),
    edgeBundles: Map[Symbol, EdgeBundle] = Map(),
    hybridBundles: Map[Symbol, HybridBundle] = Map(),
    attributes: Map[Symbol, Attribute[_]] = Map(),
    scalars: Map[Symbol, Scalar[_]] = Map(),
    tables: Map[Symbol, Table] = Map())
    extends ToJson {
  val all: Map[Symbol, MetaGraphEntity] =
    vertexSets ++ edgeBundles ++ hybridBundles ++ attributes ++ scalars ++ tables
  assert(
    all.size ==
      vertexSets.size +
      edgeBundles.size +
      hybridBundles.size +
      attributes.size +
      scalars.size +
      tables.size,
    s"Cross type collision in $this")

  def asStringMap: Map[String, String] =
    all.toSeq.sortBy(_._1.name).map {
      case (name, entity) => name.name -> entity.gUID.toString
    }.toMap
  override def toJson = {
    import play.api.libs.json.{JsObject, JsString}
    new JsObject(asStringMap.mapValues(JsString(_)))
  }

  def apply(name: Symbol) = all(name)

  def ++(mds: MetaDataSet): MetaDataSet = {
    assert(
      (all.keySet & mds.all.keySet).forall(key => all(key).gUID == mds.all(key).gUID),
      "Collision: " + (all.keySet & mds.all.keySet).toSeq.filter(key => all(key).gUID != mds.all(key).gUID),
    )
    return MetaDataSet(
      vertexSets ++ mds.vertexSets,
      edgeBundles ++ mds.edgeBundles,
      hybridBundles ++ mds.hybridBundles,
      attributes ++ mds.attributes,
      scalars ++ mds.scalars,
      tables ++ mds.tables,
    )
  }

  def mapNames(mapping: (Symbol, Symbol)*): MetaDataSet = {
    MetaDataSet(mapping.map {
      case (from, to) => to -> all(from)
    }.toMap)
  }

  override def toString = all.toString
}
object MetaDataSet {
  def apply(all: Map[Symbol, MetaGraphEntity]): MetaDataSet = {
    MetaDataSet(
      vertexSets = all.collect { case (k, v: VertexSet) => (k, v) },
      edgeBundles = all.collect { case (k, v: EdgeBundle) => (k, v) },
      hybridBundles = all.collect { case (k, v: HybridBundle) => (k, v) },
      attributes = all.collect { case (k, v: Attribute[_]) => (k, v) }.toMap,
      scalars = all.collect { case (k, v: Scalar[_]) => (k, v) }.toMap,
      tables = all.collect { case (k, v: Table) => (k, v) },
    )
  }
}
