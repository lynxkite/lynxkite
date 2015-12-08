// Fast serialization for simple types with fallback to Kryo.
// It can be faster than Kryo, because it knows the type for all the values up front.
package com.lynxanalytics.biggraph.graph_api.io

import java.nio.ByteBuffer
import org.apache.hadoop.io.BytesWritable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import scala.reflect.runtime.universe._

object EntitySerializer {
  // Beware: the same BytesWritable may be re-used in the output iterator.
  type Serializer[T] = Iterator[(ID, T)] => Iterator[(ID, BytesWritable)]

  val unitSerializer = new EntitySerializer[Unit]("unit", it => {
    it.map { case (k, v) => k -> serializedUnit }
  })

  val serializedUnit = new BytesWritable()

  val stringSerializer = new EntitySerializer[String]("string", it => {
    it.map { case (k, v) => k -> serializeString(v) }
  })

  def serializeString(s: String) = new BytesWritable(s.getBytes("utf-8"))

  val doubleSerializer = new EntitySerializer[Double]("double", it => {
    val bytes = new Array[Byte](8)
    val bw = new BytesWritable(bytes)
    val bb = ByteBuffer.wrap(bytes)
    it.map {
      case (k, v) =>
        val vv: Double = v
        bb.putDouble(0, vv)
        k -> bw
    }
  })

  val edgeSerializer = new EntitySerializer[Edge]("edge", it => {
    val bytes = new Array[Byte](16)
    val bw = new BytesWritable(bytes)
    val bb = ByteBuffer.wrap(bytes)
    it.map {
      case (k, v) =>
        bb.putLong(0, v.src)
        bb.putLong(8, v.dst)
        k -> bw
    }
  })

  def kryoSerializer[T: TypeTag] = {
    val tt = typeOf[T]
    new EntitySerializer[T](s"kryo[$tt]", it => {
      it.map { case (k, v) => k -> new BytesWritable(RDDUtils.kryoSerialize(v)) }
    })
  }

  def forType[T: TypeTag]: EntitySerializer[T] = {
    val tt = typeOf[T]
    val s: EntitySerializer[_] =
      if (tt =:= typeOf[Unit]) unitSerializer
      else if (tt =:= typeOf[String]) stringSerializer
      else if (tt =:= typeOf[Double]) doubleSerializer
      else if (tt =:= typeOf[Edge]) edgeSerializer
      else kryoSerializer[T]
    s.asInstanceOf[EntitySerializer[T]]
  }
}
class EntitySerializer[T](
  val name: String,
  val mapper: EntitySerializer.Serializer[T])

object EntityDeserializer {
  type Deserializer[T] = BytesWritable => T

  val unitDeserializer = new EntityDeserializer[Unit]("unit", _ => ())

  val kryoDeserializer = new EntityDeserializer[Any](
    "kryo", bw => RDDUtils.kryoDeserialize[Any](bw.getBytes)) {
    override def assertSupports[T: TypeTag] = {} // Works for any type.
  }

  val stringDeserializer = new EntityDeserializer[String]("string", bw => {
    new String(bw.getBytes, 0, bw.getLength, "utf-8")
  })

  val doubleDeserializer = new EntityDeserializer[Double]("double", bw => {
    val bb = ByteBuffer.wrap(bw.getBytes)
    bb.getDouble
  })

  val edgeDeserializer = new EntityDeserializer[Edge]("edge", bw => {
    val bb = ByteBuffer.wrap(bw.getBytes)
    Edge(bb.getLong(0), bb.getLong(8))
  })

  def forName[T: TypeTag](name: String): EntityDeserializer[T] = {
    val deserializers = Seq[EntityDeserializer[_]](
      unitDeserializer, kryoDeserializer, stringDeserializer, doubleDeserializer, edgeDeserializer)
    val stripped = name.replaceFirst("\\[.*", "") // Drop Kryo type note.
    val d = deserializers.find(_.name == stripped).getOrElse {
      throw new AssertionError(s"Cannot find deserializer for $name.")
    }
    d.assertSupports[T]
    d.asInstanceOf[EntityDeserializer[T]]
  }
}
class EntityDeserializer[T: TypeTag](
    val name: String,
    val mapper: EntityDeserializer.Deserializer[T]) {
  def assertSupports[T2: TypeTag] = {
    val t = typeOf[T]
    val t2 = typeOf[T2]
    assert(t =:= t2, s"EntityDeserializer $name is for $t, not $t2")
  }
}
