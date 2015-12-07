// Fast serialization for simple types.
// It can be faster than Kryo, because it knows the type for all the values up front.
package com.lynxanalytics.biggraph.graph_util

import java.nio.ByteBuffer
import org.apache.hadoop.io.BytesWritable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import scala.reflect.runtime.universe._

object SimpleSerializer {
  // Beware: the same BytesWritable may be re-used in the output iterator.
  type Serializer[T] = Iterator[(ID, T)] => Iterator[(ID, BytesWritable)]

  val unitSerializer = new SimpleSerializer[Unit]("unit", it => {
    val empty = new BytesWritable()
    it.map { case (k, v) => k -> empty }
  })

  val stringSerializer = new SimpleSerializer[String]("string", it => {
    it.map { case (k, v) => k -> new BytesWritable(v.getBytes("utf-8")) }
  })

  val doubleSerializer = new SimpleSerializer[Double]("double", it => {
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

  val edgeSerializer = new SimpleSerializer[Edge]("edge", it => {
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

  val kryoSerializer = new SimpleSerializer[Any]("kryo", it => {
    it.map { case (k, v) => k -> new BytesWritable(RDDUtils.kryoSerialize(v)) }
  })

  def forType[T: TypeTag]: SimpleSerializer[T] = {
    val tt = typeOf[T]
    val s: SimpleSerializer[_] =
      if (tt =:= typeOf[Unit]) unitSerializer
      else if (tt =:= typeOf[String]) stringSerializer
      else if (tt =:= typeOf[Double]) doubleSerializer
      else if (tt =:= typeOf[Edge]) edgeSerializer
      else kryoSerializer
    s.asInstanceOf[SimpleSerializer[T]]
  }
}
class SimpleSerializer[T](
  val name: String,
  val mapper: SimpleSerializer.Serializer[T])

object SimpleDeserializer {
  type Deserializer[T] = BytesWritable => T

  val unitDeserializer = new SimpleDeserializer[Unit]("unit", _ => ())

  val kryoDeserializer = new SimpleDeserializer[Any](
    "kryo", bw => RDDUtils.kryoDeserialize[Any](bw.getBytes)) {
    override def assertSupports[T: TypeTag] = {} // Works for any type.
  }

  val stringDeserializer = new SimpleDeserializer[String]("string", bw => {
    new String(bw.getBytes, 0, bw.getLength, "utf-8")
  })

  val doubleDeserializer = new SimpleDeserializer[Double]("double", bw => {
    val bb = ByteBuffer.wrap(bw.getBytes)
    bb.getDouble
  })

  val edgeDeserializer = new SimpleDeserializer[Edge]("edge", bw => {
    val bb = ByteBuffer.wrap(bw.getBytes)
    Edge(bb.getLong(0), bb.getLong(8))
  })

  def forName[T: TypeTag](name: String): SimpleDeserializer[T] = {
    val deserializers = Seq[SimpleDeserializer[_]](
      unitDeserializer, kryoDeserializer, stringDeserializer, doubleDeserializer, edgeDeserializer)
    val d = deserializers.find(_.name == name).get
    d.assertSupports[T]
    d.asInstanceOf[SimpleDeserializer[T]]
  }
}
class SimpleDeserializer[T: TypeTag](
    val name: String,
    val mapper: SimpleDeserializer.Deserializer[T]) {
  def assertSupports[T2: TypeTag] = {
    val t = typeOf[T]
    val t2 = typeOf[T2]
    assert(t =:= t2, s"SimpleDeserializer $name is for $t, not $t2")
  }
}
