// The SphynxDomain can connect to a Sphynx server that runs single-node operations.

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util
import play.api.libs.json.Json
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.nio.file.{ Paths, Files }
import java.io.SequenceInputStream
import scala.collection.JavaConversions.asJavaEnumeration
import org.apache.spark.rdd.RDD
import reflect.runtime.universe.typeTag
import com.lynxanalytics.biggraph.graph_util.HadoopFile

abstract class SphynxDomain(host: String, port: Int, certDir: String) extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "SphynxDomain",
      maxParallelism = graph_util.LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)
  val client = new SphynxClient(host, port, certDir)
}

class SphynxMemory(host: String, port: Int, certDir: String) extends SphynxDomain(host, port, certDir) {

  override def has(entity: MetaGraphEntity): Boolean = {
    client.hasInSphynxMemory(entity)
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    val jsonMeta = Json.stringify(MetaGraphManager.serializeOperation(instance))
    client.compute(jsonMeta).map(_ => ())
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    val jsonMeta = Json.stringify(MetaGraphManager.serializeOperation(instance))
    client.canCompute(jsonMeta)
  }

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = {
    client.getScalar(scalar)
  }

  override def cache(e: MetaGraphEntity): Unit = {
    ???
  }

  override def canRelocateFrom(source: Domain): Boolean = {
    source match {
      case _: OrderedSphynxDisk => true
      case _: UnorderedSphynxDisk => true
      case _ => false
    }
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    source match {
      case _: OrderedSphynxDisk => client.readFromOrderedSphynxDisk(e)
      case _: UnorderedSphynxDisk => client.readFromUnorderedDisk(e)
      case _ => ???
    }
  }

}

class OrderedSphynxDisk(host: String, port: Int, certDir: String) extends SphynxDomain(host, port, certDir) {

  override def has(entity: MetaGraphEntity): Boolean = {
    return client.hasOnOrderedSphynxDisk(entity)
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    ???
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    false
  }

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = {
    client.getScalar(scalar)
  }

  override def cache(e: MetaGraphEntity): Unit = {
    ???
  }

  override def canRelocateFrom(source: Domain): Boolean = {
    return false
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    ???
  }
}

abstract class UnorderedSphynxDisk(host: String, port: Int, certDir: String)
  extends SphynxDomain(host, port, certDir) {

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    ???
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    false
  }

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = {
    throw new AssertionError("UnorderedSphynxDisk never contains scalars.")
  }

  override def cache(e: MetaGraphEntity): Unit = {
    ???
  }

  def getGUIDPath(e: MetaGraphEntity): String

  def relocateFromSpark(e: MetaGraphEntity, source: SparkDomain): SafeFuture[Unit] = {
    def writeRDD(rdd: RDD[Row], schema: StructType, e: MetaGraphEntity) = {
      val dstPath = getGUIDPath(e)
      val df = source.sparkSession.createDataFrame(rdd, schema)
      df.write.parquet(dstPath)
    }
    SafeFuture.async[Unit](source.getData(e) match {
      case v: VertexSetData => {
        val rdd = v.rdd.map {
          case (k, _) => Row(k)
        }
        val schema = StructType(Seq(StructField("id", LongType, false)))
        writeRDD(rdd, schema, e)
      }
      case eb: EdgeBundleData => {
        val rdd = eb.rdd.map {
          case (id, Edge(src, dst)) => Row(id, src, dst)
        }
        val schema = StructType(Seq(
          StructField("id", LongType, false),
          StructField("src", LongType, false),
          StructField("dst", LongType, false)))
        writeRDD(rdd, schema, e)
      }
      case a: AttributeData[_] if a.typeTag == typeTag[String] => {
        val rdd = a.rdd.map {
          case (id, value) => Row(id, value)
        }
        val schema = StructType(Seq(
          StructField("id", LongType, false),
          StructField("value", StringType, false)))
        writeRDD(rdd, schema, e)
      }
      case a: AttributeData[_] if a.typeTag == typeTag[Double] => {
        val rdd = a.rdd.map {
          case (id, value) => Row(id, value)
        }
        val schema = StructType(Seq(
          StructField("id", LongType, false),
          StructField("value", DoubleType, false)))
        writeRDD(rdd, schema, e)
      }
      case a: AttributeData[_] if a.typeTag == typeTag[(Double, Double)] => {
        val rdd = a.rdd.map {
          case (id, (value1, value2)) => Row(id, value1, value2)
        }
        val schema = StructType(Seq(
          StructField("id", LongType, false),
          StructField("value1", DoubleType, false),
          StructField("value2", DoubleType, false)))
        writeRDD(rdd, schema, e)
      }
      // TODO: Relocate scalars.
      case _ => ???
    })
  }
}

class UnorderedSphynxLocalDisk(host: String, port: Int, certDir: String, val dataDir: String)
  extends UnorderedSphynxDisk(host, port, certDir) {

  override def has(entity: MetaGraphEntity): Boolean = {
    new java.io.File(s"${dataDir}/${entity.gUID.toString}").exists()
  }

  override def canRelocateFrom(source: Domain): Boolean = {
    source match {
      case _: SphynxMemory => true
      case source: SparkDomain => if (source.isLocal) true else false
      case _: UnorderedSphynxSparkDisk => true
      case _ => false
    }
  }

  override def getGUIDPath(e: MetaGraphEntity) = {
    s"${dataDir}/${e.gUID.toString}"
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    source match {
      case source: SphynxMemory => {
        e match {
          case v: VertexSet => client.writeToUnorderedDisk(v)
          case eb: EdgeBundle => client.writeToUnorderedDisk(eb)
          case a: Attribute[_] => client.writeToUnorderedDisk(a)
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        }
      }
      case source: SparkDomain => relocateFromSpark(e, source)
      case source: UnorderedSphynxSparkDisk => {
        SafeFuture.async({
          val srcPath = source.dataDir / e.gUID.toString
          val dstPath = Paths.get(s"${dataDir}/${e.gUID.toString}")
          val files = (srcPath / "part-*").list
          val stream = new SequenceInputStream(files.view.map(_.open).iterator)
          try Files.copy(stream, dstPath)
          finally stream.close()
        })
      }
    }
  }
}

class UnorderedSphynxSparkDisk(host: String, port: Int, certDir: String, val dataDir: HadoopFile)
  extends UnorderedSphynxDisk(host, port, certDir) {
  override def canRelocateFrom(source: Domain): Boolean = {
    source match {
      case _: UnorderedSphynxLocalDisk => true
      case _: SparkDomain => true
      case _ => false
    }
  }
  override def has(entity: MetaGraphEntity): Boolean = {
    false //TODO
  }

  override def getGUIDPath(e: MetaGraphEntity) = {
    (dataDir / e.gUID.toString).resolvedName
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    source match {
      case source: UnorderedSphynxLocalDisk => SafeFuture.async({
        val dstPath = dataDir / e.gUID.toString
        val srcPath = Paths.get(s"${source.dataDir}/${e.gUID.toString}")
        val stream = dstPath.create()
        try java.nio.file.Files.copy(srcPath, stream)
        finally stream.close()
      })
      case source: SparkDomain => relocateFromSpark(e, source)
    }
  }
}
