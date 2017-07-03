// The core classes for representing our data.
//
// The "metagraph" is made up of MetaGraphEntities (vertex sets, edge bundles,
// attributes, scalars, and tables) and MetaGraphOperationInstances. The entities are
// the inputs and outputs of the operation instances. The operation instance is
// an operation that is bound to a set of inputs.
//
// This file also includes the machinery for declaring operation input/output
// signatures, and the "data" classes that are the actual RDDs/scalar values
// that belong to metagraph entities.

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
  // Implement from source operation's GUID, name and the actual class of this component.
  lazy val gUID: UUID = {
    val buffer = new ByteArrayOutputStream
    val objectStream = new ObjectOutputStream(buffer)
    objectStream.writeObject(name)
    objectStream.writeObject(source.gUID)
    objectStream.writeObject(this.getClass.toString)
    objectStream.close()
    UUID.nameUUIDFromBytes(buffer.toByteArray)
  }
  override def toString = s"$gUID (${name.name} of $source)"
  lazy val toStringStruct = StringStruct(name.name, Map("" -> source.toStringStruct))
  def manager = source.manager
}
case class StringStruct(name: String, contents: SortedMap[String, StringStruct] = SortedMap()) {
  lazy val asString: String = {
    val stuff = contents.map {
      case (k, v) =>
        val s = v.asString
        val guarded = if (s.contains(" ")) s"($s)" else s
        if (k.isEmpty) guarded else s"$k=$guarded"
    }.mkString(" ")
    if (stuff.isEmpty) name else s"$name of $stuff"
  }
  override def toString = asString
}
object StringStruct {
  def apply(name: String, contents: Map[String, StringStruct]) =
    new StringStruct(name, SortedMap[String, StringStruct]() ++ contents)
}

case class VertexSet(source: MetaGraphOperationInstance,
                     name: Symbol) extends MetaGraphEntity {
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

case class EdgeBundle(source: MetaGraphOperationInstance,
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

case class HybridBundle(source: MetaGraphOperationInstance,
                        name: Symbol,
                        edgeBundle: EdgeBundle) // The RDD for idSet will be auto-generated.
    extends MetaGraphEntity {
  assert(name != null, s"name is null for $this")
}

sealed trait TypedEntity[T] extends MetaGraphEntity {
  val typeTag: TypeTag[T]
  def runtimeSafeCast[S: TypeTag]: TypedEntity[S]
  def is[S: TypeTag]: Boolean
}

case class Attribute[T: TypeTag](source: MetaGraphOperationInstance,
                                 name: Symbol,
                                 vertexSet: VertexSet)
    extends TypedEntity[T] with RuntimeSafeCastable[T, Attribute] {
  assert(name != null, s"name is null for $this")
  val typeTag = implicitly[TypeTag[T]]
}

case class Scalar[T: TypeTag](source: MetaGraphOperationInstance,
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

trait FieldNaming {
  private lazy val naming: IdentityHashMap[Any, Symbol] = {
    val res = new IdentityHashMap[Any, Symbol]()
    val mirror = reflect.runtime.currentMirror.reflect(this)

    val fields = mirror.symbol.toType.members.collect {
      case m: MethodSymbol if (m.isGetter && m.isPublic) => m
    }
    for (m <- fields) {
      res.put(mirror.reflectField(m).get, Symbol(m.name.toString))
    }
    res
  }
  def nameOf(obj: Any): Symbol = {
    val name = naming.get(obj)
    assert(
      name != null,
      "This is typically caused by a name being used before the initialization of " +
        "the FieldNaming subclass. We were looking for the name of: %s. Available names: %s".format(
          obj, naming))
    name
  }
}

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
    override def set(target: MetaDataSet, heb: HybridBundle): MetaDataSet = {
      val withEs =
        templatesByName(es).asInstanceOf[EdgeBundleTemplate].set(target, heb.edgeBundle)
      super.set(withEs, heb)
    }
    def data(implicit dataSet: DataSet) = dataSet.HybridBundles(name)
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

  class AnyVertexAttributeTemplate(vsF: => Symbol, nameOpt: Option[Symbol])
      extends ET[Attribute[_]](nameOpt) {
    lazy val vs = vsF
    override def set(target: MetaDataSet, va: Attribute[_]): MetaDataSet = {
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
      assert(target.edgeBundles.contains(es),
        s"The edge bundle input ($es) has to be provided before the attribute ($name).")
      val eb = templatesByName(es).asInstanceOf[EdgeBundleTemplate].get(target)
      assert(eb.idSet == ea.vertexSet,
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

  class TableTemplate(nameOpt: Option[Symbol]) extends ET[Table](nameOpt) {
    def data(implicit dataSet: DataSet) = dataSet.tables(name).asInstanceOf[TableData]
    def df(implicit dataSet: DataSet) = data.df
  }

  def vertexSet = new VertexSetTemplate(None)
  def vertexSet(name: Symbol) = new VertexSetTemplate(Some(name))
  def edgeBundle(
    src: VertexSetTemplate,
    dst: VertexSetTemplate,
    requiredProperties: EdgeBundleProperties = EdgeBundleProperties.default,
    idSet: VertexSetTemplate = null,
    name: Symbol = null) =
    new EdgeBundleTemplate(
      src.name, dst.name, Option(idSet).map(_.name), requiredProperties, Option(name))
  def hybridBundle(
    es: EdgeBundleTemplate,
    name: Symbol = null) =
    new HybridBundleTemplate(
      es.name, Option(name))
  def vertexAttribute[T](vs: VertexSetTemplate, name: Symbol = null) =
    new VertexAttributeTemplate[T](vs.name, Option(name))
  def anyVertexAttribute(vs: VertexSetTemplate, name: Symbol = null) =
    new AnyVertexAttributeTemplate(vs.name, Option(name))
  def edgeAttribute[T](es: EdgeBundleTemplate, name: Symbol = null) =
    new EdgeAttributeTemplate[T](es.name, Option(name))
  def scalar[T] = new ScalarTemplate[T](None)
  def scalar[T](name: Symbol) = new ScalarTemplate[T](Some(name))
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
      hybridBundles = templates.collect { case heb: HybridBundleTemplate => heb.name }.toSet,
      attributes = templates.collect {
        case a: VertexAttributeTemplate[_] => a.name
        case a: EdgeAttributeTemplate[_] => a.name
        case a: AnyVertexAttributeTemplate => a.name
      }.toSet,
      scalars = templates.collect { case sc: ScalarTemplate[_] => sc.name }.toSet,
      tables = templates.collect { case tb: TableTemplate => tb.name }.toSet)

  private val templates = mutable.Buffer[ET[_ <: MetaGraphEntity]]()
  private lazy val templatesByName = {
    val pairs: Iterable[(Symbol, ET[_ <: MetaGraphEntity])] =
      templates.map(t => (t.name, t))
    pairs.toMap
  }
}
trait MetaDataSetProvider {
  def metaDataSet: MetaDataSet
}

trait EntityContainer[+T <: MetaGraphEntity] {
  def entity: T
}
object EntityContainer {
  implicit class TrivialContainer[T <: MetaGraphEntity](val entity: T) extends EntityContainer[T]
  import scala.language.implicitConversions
  implicit def unpackContainer[T <: MetaGraphEntity](container: EntityContainer[T]): T =
    container.entity
}

abstract class MagicOutput(instance: MetaGraphOperationInstance)
    extends MetaDataSetProvider with FieldNaming {
  class P[T <: MetaGraphEntity](entityConstructor: Symbol => T, nameOpt: Option[Symbol]) extends EntityContainer[T] {
    lazy val name: Symbol = {
      val name = nameOpt.getOrElse(nameOf(this))
      assert(!name.name.endsWith("-idSet"),
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
    eb = new P(EdgeBundle(
      instance, _, src, dst, properties,
      idSetSafe, autogenerateIdSet = idSet == null), Option(name))
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
trait MetaGraphOp extends Serializable with ToJson {
  // An operation is heavy if it is faster to load its results than it is to recalculate them.
  // Heavy operation outputs are written out and loaded back on completion.
  val isHeavy: Boolean = false
  val neverSerialize: Boolean = false
  assert(!isHeavy || !neverSerialize, "$this cannot be heavy and never serialize at the same time")
  // If a heavy operation hasCustomSaving, it can just write out some or all of its outputs
  // instead of putting them in the OutputBuilder in execute().
  val hasCustomSaving: Boolean = false
  assert(!hasCustomSaving || isHeavy, "$this cannot have custom saving if it is not heavy.")

  def inputSig: InputSignature
  def outputMeta(instance: MetaGraphOperationInstance): MetaDataSetProvider

  val gUID = {
    val contents = this.toTypedJson.toString
    val version = JsonMigration.current.version(getClass.getName)
    UUID.nameUUIDFromBytes((contents + version).getBytes(MetaGraphOp.UTF8))
  }

  def toStringStruct = {
    val mirror = reflect.runtime.currentMirror.reflect(this)
    val className = mirror.symbol.name.toString
    val params = mirror.symbol.toType.members.collect { case m: MethodSymbol if m.isCaseAccessor => m }
    def get(param: MethodSymbol) = mirror.reflectField(param).get
    StringStruct(
      className, params.map(p => p.name.toString -> StringStruct(get(p).toString)).toMap)
  }
}

object TypedMetaGraphOp {
  // A little "hint" for the type inference.
  type Type = TypedMetaGraphOp[_ <: InputSignatureProvider, _ <: MetaDataSetProvider]
}
trait TypedMetaGraphOp[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider]
    extends MetaGraphOp {
  def inputs: IS = ???
  def inputSig: InputSignature = inputs.inputSignature
  def outputMeta(instance: MetaGraphOperationInstance): OMDS

  def execute(
    inputDatas: DataSet,
    outputMeta: OMDS,
    output: OutputBuilder,
    rc: RuntimeContext): Unit
}

/*
 * Base class for concrete instances of MetaGraphOperations. An instance of an operation is
 * the operation together with concrete input vertex sets and edge bundles.
 */
trait MetaGraphOperationInstance {
  val manager: MetaGraphManager

  val operation: MetaGraphOp

  val inputs: MetaDataSet

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

  def run(inputDatas: DataSet, runtimeContext: RuntimeContext): Map[UUID, EntityData]

  override def toString = s"$gUID ($operation)"
  lazy val toStringStruct: StringStruct = {
    val op = operation.toStringStruct
    val fixed = mutable.Set[UUID]()
    val mentioned = mutable.Map[UUID, Symbol]()
    val span = mutable.Map[String, StringStruct]()
    def put(k: Symbol, v: MetaGraphEntity): Unit = {
      if (!fixed.contains(v.gUID)) {
        mentioned.get(v.gUID) match {
          case Some(k0) =>
            span(k.name) = StringStruct(k0.name)
          case None =>
            span(k.name) = v.toStringStruct
            mentioned(v.gUID) = k
        }
      }
    }
    for ((k, v) <- inputs.edgeBundles) {
      put(k, v)
      fixed += v.srcVertexSet.gUID
      fixed += v.dstVertexSet.gUID
      fixed += v.idSet.gUID
    }
    for ((k, v) <- inputs.attributes) {
      put(k, v)
      fixed += v.vertexSet.gUID
    }
    for ((k, v) <- inputs.vertexSets) {
      put(k, v)
    }
    StringStruct(op.name, op.contents ++ span)
  }
}

case class TypedOperationInstance[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    manager: MetaGraphManager,
    operation: TypedMetaGraphOp[IS, OMDS],
    inputs: MetaDataSet) extends MetaGraphOperationInstance {
  val result: OMDS = operation.outputMeta(this)
  val outputs: MetaDataSet = result.metaDataSet
  def run(inputDatas: DataSet, runtimeContext: RuntimeContext): Map[UUID, EntityData] = {
    val outputBuilder = new OutputBuilder(this)
    operation.execute(inputDatas, result, outputBuilder, runtimeContext)
    outputBuilder.dataMap.toMap
  }
  override lazy val hashCode = gUID.hashCode
}

sealed trait EntityData {
  val entity: MetaGraphEntity
  def gUID = entity.gUID
}
sealed trait EntityRDDData[T] extends EntityData {
  val rdd: org.apache.spark.rdd.RDD[(ID, T)]
  val count: Option[Long]
  def cached: EntityRDDData[T]
  rdd.setName("RDD[%d]/%d of %s GUID[%s]".format(rdd.id, rdd.partitions.size, entity, gUID))
}
class VertexSetData(val entity: VertexSet,
                    val rdd: VertexSetRDD,
                    val count: Option[Long] = None) extends EntityRDDData[Unit] {
  val vertexSet = entity
  def cached = new VertexSetData(entity, rdd.copyWithAncestorsCached, count)
}

class EdgeBundleData(val entity: EdgeBundle,
                     val rdd: EdgeBundleRDD,
                     val count: Option[Long] = None) extends EntityRDDData[Edge] {
  val edgeBundle = entity
  def cached = new EdgeBundleData(entity, rdd.copyWithAncestorsCached, count)
}

class HybridBundleData(val entity: HybridBundle,
                       val rdd: HybridBundleRDD,
                       val count: Option[Long] = None) extends EntityRDDData[ID] {
  val HybridBundle = entity
  def cached = new HybridBundleData(entity, rdd.persist(spark.storage.StorageLevel.MEMORY_ONLY), count)
}

class AttributeData[T](val entity: Attribute[T],
                       val rdd: AttributeRDD[T],
                       val count: Option[Long] = None)
    extends EntityRDDData[T] with RuntimeSafeCastable[T, AttributeData] {
  val attribute = entity
  val typeTag = attribute.typeTag
  def cached = new AttributeData[T](entity, rdd.copyWithAncestorsCached, count)
}

class ScalarData[T](val entity: Scalar[T],
                    val value: T)
    extends EntityData with RuntimeSafeCastable[T, ScalarData] {
  val scalar = entity
  val typeTag = scalar.typeTag
}

class TableData(val entity: Table,
                val df: spark.sql.DataFrame)
    extends EntityData {
  val table = entity
}

// A bundle of metadata types.
case class MetaDataSet(vertexSets: Map[Symbol, VertexSet] = Map(),
                       edgeBundles: Map[Symbol, EdgeBundle] = Map(),
                       HybridBundles: Map[Symbol, HybridBundle] = Map(),
                       attributes: Map[Symbol, Attribute[_]] = Map(),
                       scalars: Map[Symbol, Scalar[_]] = Map(),
                       tables: Map[Symbol, Table] = Map())
    extends ToJson {
  val all: Map[Symbol, MetaGraphEntity] =
    vertexSets ++ edgeBundles ++ HybridBundles ++ attributes ++ scalars ++ tables
  assert(all.size ==
    vertexSets.size + edgeBundles.size + HybridBundles.size + attributes.size + scalars.size + tables.size,
    s"Cross type collision in $this")

  def asStringMap: Map[String, String] =
    all.toSeq.sortBy(_._1.name).map {
      case (name, entity) => name.name -> entity.gUID.toString
    }.toMap
  override def toJson = {
    import play.api.libs.json.{ JsObject, JsString }
    new JsObject(asStringMap.mapValues(JsString(_)).toSeq)
  }

  def apply(name: Symbol) = all(name)

  def ++(mds: MetaDataSet): MetaDataSet = {
    assert(
      (all.keySet & mds.all.keySet).forall(key => all(key).gUID == mds.all(key).gUID),
      "Collision: " + (all.keySet & mds.all.keySet).toSeq.filter(
        key => all(key).gUID != mds.all(key).gUID))
    return MetaDataSet(
      vertexSets ++ mds.vertexSets,
      edgeBundles ++ mds.edgeBundles,
      HybridBundles ++ mds.HybridBundles,
      attributes ++ mds.attributes,
      scalars ++ mds.scalars,
      tables ++ mds.tables)
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
      HybridBundles = all.collect { case (k, v: HybridBundle) => (k, v) },
      attributes = all.collect { case (k, v: Attribute[_]) => (k, v) }.toMap,
      scalars = all.collect { case (k, v: Scalar[_]) => (k, v) }.toMap,
      tables = all.collect { case (k, v: Table) => (k, v) })
  }
}

// A bundle of data types.
case class DataSet(vertexSets: Map[Symbol, VertexSetData] = Map(),
                   edgeBundles: Map[Symbol, EdgeBundleData] = Map(),
                   HybridBundles: Map[Symbol, HybridBundleData] = Map(),
                   attributes: Map[Symbol, AttributeData[_]] = Map(),
                   scalars: Map[Symbol, ScalarData[_]] = Map(),
                   tables: Map[Symbol, TableData] = Map()) {
  def metaDataSet = MetaDataSet(
    vertexSets.mapValues(_.vertexSet),
    edgeBundles.mapValues(_.edgeBundle),
    HybridBundles.mapValues(_.HybridBundle),
    attributes.mapValues(_.attribute),
    scalars.mapValues(_.scalar),
    tables.mapValues(_.table))

  def all: Map[Symbol, EntityData] =
    vertexSets ++ edgeBundles ++ HybridBundles ++ attributes ++ scalars ++ tables
}

object DataSet {
  def apply(all: Map[Symbol, EntityData]): DataSet = {
    DataSet(
      vertexSets = all.collect { case (k, v: VertexSetData) => (k, v) },
      edgeBundles = all.collect { case (k, v: EdgeBundleData) => (k, v) },
      HybridBundles = all.collect { case (k, v: HybridBundleData) => (k, v) },
      attributes = all.collect { case (k, v: AttributeData[_]) => (k, v) }.toMap,
      scalars = all.collect { case (k, v: ScalarData[_]) => (k, v) }.toMap,
      tables = all.collect { case (k, v: TableData) => (k, v) })
  }
}

class OutputBuilder(val instance: MetaGraphOperationInstance) {
  val outputMeta: MetaDataSet = instance.outputs

  def addData(data: EntityData): Unit = {
    val gUID = data.gUID
    val entity = data.entity
    // Check that it's indeed a known output.
    assert(outputMeta.all(entity.name).gUID == entity.gUID,
      s"$entity is not an output of $instance")
    internalDataMap(gUID) = data
  }

  def apply(vertexSet: VertexSet, rdd: VertexSetRDD): Unit = {
    addData(new VertexSetData(vertexSet, rdd))
  }

  def apply(edgeBundle: EdgeBundle, rdd: EdgeBundleRDD): Unit = {
    addData(new EdgeBundleData(edgeBundle, rdd))
    if (edgeBundle.autogenerateIdSet) {
      addData(new VertexSetData(edgeBundle.idSet, rdd.mapValues(_ => ())))
    }
  }

  def apply(HybridBundle: HybridBundle, rdd: HybridBundleRDD): Unit = {
    addData(new HybridBundleData(HybridBundle, rdd))
  }

  def apply[T](attribute: Attribute[T], rdd: AttributeRDD[T]): Unit = {
    addData(new AttributeData(attribute, rdd))
  }

  def apply[T](scalar: Scalar[T], value: T): Unit = {
    addData(new ScalarData(scalar, value))
  }

  def apply(table: Table, df: spark.sql.DataFrame): Unit = {
    import com.lynxanalytics.biggraph.spark_util.SQLHelper
    assert(SQLHelper.allNullable(table.schema) == SQLHelper.allNullable(df.schema),
      s"Output schema mismatch. Declared: ${table.schema} Output: ${df.schema}")
    addData(new TableData(table, df))
  }

  def dataMap() = {
    if (!instance.operation.hasCustomSaving) {
      val missing = outputMeta.all.values.filter(x => !internalDataMap.contains(x.gUID))
      assert(missing.isEmpty, s"Output data missing for: $missing")
    }
    internalDataMap
  }

  private val internalDataMap = mutable.Map[UUID, EntityData]()
}
