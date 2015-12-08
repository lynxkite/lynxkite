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
      else new KryoSerializer[T]
    s.asInstanceOf[EntitySerializer[T]]
  }
}
abstract class EntitySerializer[T](val name: String) extends Serializable {
  // Beware: it may re-use the same BytesWritable for all calls.
  def serialize(t: T): BytesWritable
}

class UnitSerializer extends EntitySerializer[Unit]("unit") {
  @transient lazy val empty = new BytesWritable()
  def serialize(t: Unit) = empty
}

class StringSerializer extends EntitySerializer[String]("string") {
  def serialize(t: String) = new BytesWritable(t.getBytes("utf-8"))
}

class DoubleSerializer extends EntitySerializer[Double]("double") {
  @transient lazy val bytes = new Array[Byte](8)
  @transient lazy val bw = new BytesWritable(bytes)
  @transient lazy val bb = ByteBuffer.wrap(bytes)
  def serialize(t: Double) = {
    bb.putDouble(0, t)
    bw
  }
}

class EdgeSerializer extends EntitySerializer[Edge]("edge") {
  @transient lazy val bytes = new Array[Byte](16)
  @transient lazy val bw = new BytesWritable(bytes)
  @transient lazy val bb = ByteBuffer.wrap(bytes)
  def serialize(t: Edge) = {
    bb.putLong(0, t.src)
    bb.putLong(8, t.dst)
    bw
  }
}

class KryoSerializer[T: TypeTag] extends EntitySerializer[T](s"kryo[${typeOf[T]}]") {
  def serialize(t: T) = {
    new BytesWritable(RDDUtils.kryoSerialize(t))
  }
}

object EntityDeserializer {
  def forName[T: TypeTag](name: String): EntityDeserializer[T] = {
    val stripped = name.replaceFirst("\\[.*", "") // Drop Kryo type note.
    val d = stripped match {
      case "unit" => new UnitDeserializer
      case "string" => new StringDeserializer
      case "double" => new DoubleDeserializer
      case "edge" => new EdgeDeserializer
      case "kryo" => new KryoDeserializer
      case _ => throw new AssertionError(s"Cannot find deserializer for $name.")
    }
    assert(d.name == stripped, s"Bad deserializer mapping. $name mapped to ${d.name}.")
    d.assertSupports[T]
    d.asInstanceOf[EntityDeserializer[T]]
  }
}
// The API is nicer if this can be sent to the executors. The only problem is the TypeTag, as
// it is not Serializable. It is only used during creation though, so we just make it @transient.
abstract class EntityDeserializer[T](val name: String)(implicit @transient tt: TypeTag[T])
    extends Serializable {
  def deserialize(bw: BytesWritable): T
  protected def assertSupports[T2: TypeTag] = {
    val t = typeOf[T]
    val t2 = typeOf[T2]
    assert(t =:= t2, s"EntityDeserializer $name is for $t, not $t2")
  }
}

class UnitDeserializer extends EntityDeserializer[Unit]("unit") {
  def deserialize(bw: BytesWritable) = ()
}

class KryoDeserializer extends EntityDeserializer[Any]("kryo") {
  def deserialize(bw: BytesWritable) = RDDUtils.kryoDeserialize[Any](bw.getBytes)
  override def assertSupports[T: TypeTag] = {} // Works for any type.
}

class StringDeserializer extends EntityDeserializer[String]("string") {
  def deserialize(bw: BytesWritable) = new String(bw.getBytes, 0, bw.getLength, "utf-8")
}

class DoubleDeserializer extends EntityDeserializer[Double]("double") {
  def deserialize(bw: BytesWritable) = {
    val bb = ByteBuffer.wrap(bw.getBytes)
    bb.getDouble
  }
}

class EdgeDeserializer extends EntityDeserializer[Edge]("edge") {
  def deserialize(bw: BytesWritable) = {
    val bb = ByteBuffer.wrap(bw.getBytes)
    Edge(bb.getLong(0), bb.getLong(8))
  }
}
