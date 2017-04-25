// Fast serialization for simple types with fallback to Kryo.
// It can be faster than Kryo, because it knows the type for all the values up front.
package com.lynxanalytics.biggraph.graph_api.io

import java.nio.ByteBuffer
import org.apache.hadoop.io.BytesWritable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import scala.reflect.runtime.universe._

object EntitySerializer {
  def forType[T: TypeTag]: EntitySerializer[T] = {
    val tt = typeOf[T]
    val s: EntitySerializer[_] =
      if (tt =:= typeOf[Unit]) new UnitSerializer
      else if (tt =:= typeOf[String]) new StringSerializer
      else if (tt =:= typeOf[Double]) new DoubleSerializer
      else if (tt =:= typeOf[Edge]) new EdgeSerializer
      else if (TypeTagUtil.isOfKind1[Set](tt)) new SetSerializer
      else new KryoSerializer[T]
    s.asInstanceOf[EntitySerializer[T]]
  }

  def forAttribute[T](attribute: Attribute[T]): EntitySerializer[T] = {
    forType[T](attribute.typeTag)
  }
}
abstract class EntitySerializer[-T](val name: String) extends Serializable {
  // Beware: it may re-use the same BytesWritable for all calls.
  def serialize(t: T): BytesWritable

  // This can be used when the type system does not understand that t is of the right type.
  // It still, of course, has to be of type T actually.
  def unsafeSerialize(t: Any): BytesWritable = serialize(t.asInstanceOf[T])
}

class UnitSerializer extends EntitySerializer[Unit]("Unit") {
  @transient lazy val empty = new BytesWritable()
  def serialize(t: Unit) = empty
}

class StringSerializer extends EntitySerializer[String]("String") {
  def serialize(t: String) = new BytesWritable(t.getBytes("utf-8"))
}

class DoubleSerializer extends EntitySerializer[Double]("Double") {
  @transient lazy val bytes = new Array[Byte](8)
  @transient lazy val bw = new BytesWritable(bytes)
  @transient lazy val bb = ByteBuffer.wrap(bytes)
  def serialize(t: Double) = {
    bb.putDouble(0, t)
    bw
  }
}

class EdgeSerializer extends EntitySerializer[Edge]("Edge") {
  @transient lazy val bytes = new Array[Byte](16)
  @transient lazy val bw = new BytesWritable(bytes)
  @transient lazy val bb = ByteBuffer.wrap(bytes)
  def serialize(t: Edge) = {
    bb.putLong(0, t.src)
    bb.putLong(8, t.dst)
    bw
  }
}

// This serializer is not for speed, but for reliability. Scala sets are complex types internally
// and we don't know the full set of classes that we would need to register with Kryo to be sure
// we can successfully serialize them.
class SetSerializer extends EntitySerializer[Set[_]]("Set") {
  def serialize(t: Set[_]) = {
    new BytesWritable(RDDUtils.kryoSerialize(t.toVector))
  }
}

class KryoSerializer[T: TypeTag] extends EntitySerializer[T](s"Kryo[${typeOf[T]}]") {
  def serialize(t: T) = {
    new BytesWritable(RDDUtils.kryoSerialize(t))
  }
}

object EntityDeserializer {
  def castDeserializer[From: TypeTag, To: TypeTag](
    d: EntityDeserializer[From]): EntityDeserializer[To] = {
    val from = typeOf[From]
    val to = typeOf[To]
    assert(from =:= to, s"${d.name} is for $from, not $to")
    d.asInstanceOf[EntityDeserializer[To]]
  }

  def forName[T: TypeTag](name: String): EntityDeserializer[T] = {
    val stripped = name.replaceFirst("\\[.*", "") // Drop Kryo type note.
    val d: EntityDeserializer[T] = stripped match {
      case "Unit" => castDeserializer(new UnitDeserializer)
      case "String" => castDeserializer(new StringDeserializer)
      case "Double" => castDeserializer(new DoubleDeserializer)
      case "Edge" => castDeserializer(new EdgeDeserializer)
      case "Set" => SetDeserializer[T]()
      case "Kryo" => new KryoDeserializer[T]
      case _ => throw new AssertionError(s"Cannot find deserializer for $name.")
    }
    assert(d.name == stripped, s"Bad deserializer mapping. $name mapped to ${d.name}.")
    d
  }
}
abstract class EntityDeserializer[+T](val name: String) extends Serializable {
  def deserialize(bw: BytesWritable): T
}

class UnitDeserializer extends EntityDeserializer[Unit]("Unit") {
  def deserialize(bw: BytesWritable) = ()
}

class KryoDeserializer[T] extends EntityDeserializer[T]("Kryo") {
  def deserialize(bw: BytesWritable) = RDDUtils.kryoDeserialize[T](bw.getBytes)
}

class StringDeserializer extends EntityDeserializer[String]("String") {
  def deserialize(bw: BytesWritable) = new String(bw.getBytes, 0, bw.getLength, "utf-8")
}

class DoubleDeserializer extends EntityDeserializer[Double]("Double") {
  def deserialize(bw: BytesWritable) = {
    val bb = ByteBuffer.wrap(bw.getBytes)
    bb.getDouble
  }
}

class EdgeDeserializer extends EntityDeserializer[Edge]("Edge") {
  def deserialize(bw: BytesWritable) = {
    val bb = ByteBuffer.wrap(bw.getBytes)
    Edge(bb.getLong(0), bb.getLong(8))
  }
}

object SetDeserializer {
  private def fromTypeTag[To: TypeTag, FromDT: TypeTag]: EntityDeserializer[To] = {
    val d = new SetDeserializer[FromDT]
    val st = TypeTagUtil.setTypeTag[FromDT]
    EntityDeserializer.castDeserializer(d)(st, typeTag[To])
  }

  def apply[T: TypeTag]() = {
    val tt = typeTag[T]
    val args = TypeTagUtil.typeArgs(tt)
    // If it has more or less than 1 type args, it is definitely not a Set.
    assert(args.size == 1, s"Set is for Set[_], not ${tt.tpe}")
    fromTypeTag(tt, args(0))
  }
}
class SetDeserializer[DT] extends EntityDeserializer[Set[DT]]("Set") {
  def deserialize(bw: BytesWritable) = RDDUtils.kryoDeserialize[Vector[DT]](bw.getBytes).toSet
}
