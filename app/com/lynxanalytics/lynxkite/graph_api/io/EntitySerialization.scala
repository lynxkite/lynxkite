// Parquet serialization for simple types with fallback to Kryo.
// It can be faster than Kryo, because it knows the type for all the values up front.
package com.lynxanalytics.lynxkite.graph_api.io

import java.nio.ByteBuffer
import org.apache.spark
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.RDDUtils
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
abstract class EntitySerializer[T: TypeTag](val name: String) extends Serializable {
  def df[P <: Product: TypeTag](rdd: spark.rdd.RDD[P]): spark.sql.DataFrame = {
    val ss = spark.sql.SparkSession.builder.config(rdd.context.getConf).getOrCreate()
    ss.createDataFrame(rdd)
  }
  def serialize(rdd: spark.rdd.RDD[(Long, T)]): spark.sql.DataFrame = df(rdd)
}

class UnitSerializer extends EntitySerializer[Unit]("Unit") {
  override def serialize(rdd: spark.rdd.RDD[(Long, Unit)]) = df(rdd.map { case (v, _) => Tuple1(v) })
}

class StringSerializer extends EntitySerializer[String]("String")
class DoubleSerializer extends EntitySerializer[Double]("Double")

class EdgeSerializer extends EntitySerializer[Edge]("Edge") {
  override def serialize(rdd: spark.rdd.RDD[(Long, Edge)]) =
    df(rdd.map { case (e, Edge(src, dst)) => (e, src, dst) })
}

// This serializer is not for speed, but for reliability. Scala sets are complex types internally
// and we don't know the full set of classes that we would need to register with Kryo to be sure
// we can successfully serialize them.
class SetSerializer extends EntitySerializer[Set[_]]("Set") {
  override def serialize(rdd: spark.rdd.RDD[(Long, Set[_])]) =
    df(rdd.mapValues(t => RDDUtils.kryoSerialize(t.toVector)))
}

class KryoSerializer[T: TypeTag] extends EntitySerializer[T](s"Kryo[${typeOf[T]}]") {
  override def serialize(rdd: spark.rdd.RDD[(Long, T)]) =
    df(rdd.map { case (k, v) => k -> RDDUtils.kryoSerialize(v) })
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
abstract class EntityDeserializer[T](val name: String) extends Serializable {
  def deserialize(df: spark.sql.DataFrame): spark.rdd.RDD[(Long, T)]
}

class UnitDeserializer extends EntityDeserializer[Unit]("Unit") {
  def deserialize(df: spark.sql.DataFrame) = df.rdd.map(row => (row.getLong(0), ()))
}

class KryoDeserializer[T] extends EntityDeserializer[T]("Kryo") {
  def deserialize(df: spark.sql.DataFrame) =
    df.rdd.map(row => row.getLong(0) -> RDDUtils.kryoDeserialize[T](row.getAs[Array[Byte]](1)))
}

class StringDeserializer extends EntityDeserializer[String]("String") {
  def deserialize(df: spark.sql.DataFrame) =
    df.rdd.map(row => row.getLong(0) -> row.getString(1))
}

class DoubleDeserializer extends EntityDeserializer[Double]("Double") {
  def deserialize(df: spark.sql.DataFrame) =
    df.rdd.map(row => row.getLong(0) -> row.getDouble(1))
}

class EdgeDeserializer extends EntityDeserializer[Edge]("Edge") {
  def deserialize(df: spark.sql.DataFrame) =
    df.rdd.map(row => row.getLong(0) -> Edge(row.getLong(1), row.getLong(2)))
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
  def deserialize(df: spark.sql.DataFrame) =
    df.rdd.map(row =>
      row.getLong(0) ->
        RDDUtils.kryoDeserialize[Vector[DT]](row.getAs[Array[Byte]](1)).toSet)
}
