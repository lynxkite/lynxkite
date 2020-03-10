// The SphynxDomain can connect to a Sphynx server that runs single-node operations.

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util
import play.api.libs.json.Json
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.nio.file.{ Paths, Files }
import java.io.{ SequenceInputStream, File, FileInputStream }
import scala.collection.JavaConversions.asJavaEnumeration
import org.apache.spark.rdd.RDD
import reflect.runtime.universe.typeTag
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import scala.util.{ Try, Success, Failure }
import java.io.{ FileWriter, BufferedWriter }

abstract class SphynxDomain(host: String, port: Int, certDir: String) extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "SphynxDomain",
      maxParallelism = graph_util.LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)
  val client = new SphynxClient(host, port, certDir)
  val supportedTypes = List(
    typeTag[String], typeTag[Long], typeTag[Double], typeTag[(Double, Double)], typeTag[Vector[Double]])
}

class SphynxMemory(host: String, port: Int, certDir: String) extends SphynxDomain(host, port, certDir) {

  override def has(entity: MetaGraphEntity): Boolean = {
    client.hasInSphynxMemory(entity)
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    val jsonMeta = Json.stringify(MetaGraphManager.serializeOperation(instance))
    client.compute(jsonMeta, "SphynxMemory").map(_ => ())
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    for (e <- instance.inputs.attributes.values) {
      if (!supportedTypes.contains(e.typeTag)) {
        return false
      }
    }
    val jsonMeta = Json.stringify(MetaGraphManager.serializeOperation(instance))
    client.canCompute(jsonMeta, "SphynxMemory")
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
      case _: UnorderedSphynxLocalDisk => true
      case _ => false
    }
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    source match {
      case _: OrderedSphynxDisk => client.readFromOrderedSphynxDisk(e)
      case _: UnorderedSphynxLocalDisk => client.readFromUnorderedDisk(e)
      case _ => ???
    }
  }

}

class OrderedSphynxDisk(host: String, port: Int, certDir: String) extends SphynxDomain(host, port, certDir) {

  override def has(entity: MetaGraphEntity): Boolean = {
    return client.hasOnOrderedSphynxDisk(entity)
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    val jsonMeta = Json.stringify(MetaGraphManager.serializeOperation(instance))
    client.compute(jsonMeta, "OrderedSphynxDisk").map(_ => ())
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    for (e <- instance.inputs.attributes.values) {
      if (!supportedTypes.contains(e.typeTag)) {
        return false
      }
    }
    val jsonMeta = Json.stringify(MetaGraphManager.serializeOperation(instance))
    client.canCompute(jsonMeta, "OrderedSphynxDisk")
  }

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = ???
  override def canGet[T](s: Scalar[T]): Boolean = false

  override def cache(e: MetaGraphEntity): Unit = {
    ???
  }

  override def canRelocateFrom(source: Domain): Boolean = {
    return source.isInstanceOf[SphynxMemory]
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    assert(source.isInstanceOf[SphynxMemory], s"Cannot fetch $e from $source")
    client.writeToOrderedDisk(e)
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

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = ???
  override def canGet[T](s: Scalar[T]): Boolean = false

  override def cache(e: MetaGraphEntity): Unit = {
    ???
  }

  def getGUIDPath(e: MetaGraphEntity): String

  def relocateFromSpark(e: MetaGraphEntity, source: SparkDomain) = SafeFuture.async[Unit] {
    def writeRDD(rdd: RDD[Row], schema: StructType, e: MetaGraphEntity) = {
      val dstPath = getGUIDPath(e)
      val df = source.sparkSession.createDataFrame(rdd, schema)
      df.write.parquet(dstPath)
    }
    source.getData(e) match {
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
      case a: AttributeData[_] if a.typeTag == typeTag[Long] => {
        val rdd = a.rdd.map {
          case (id, value) => Row(id, value)
        }
        val schema = StructType(Seq(
          StructField("id", LongType, false),
          StructField("value", LongType, false)))
        writeRDD(rdd, schema, e)
      }
      case a: AttributeData[_] if a.typeTag == typeTag[(Double, Double)] => {
        val rdd = a.rdd.map {
          case (id, (x, y)) => Row(id, Row.fromSeq(List(x, y)))
        }
        val schema = StructType(Seq(
          StructField("id", LongType, false),
          StructField("value", StructType(
            Seq(
              StructField("x", DoubleType, false),
              StructField("y", DoubleType, false))), false)))
        writeRDD(rdd, schema, e)
      }
      case a: AttributeData[_] if a.typeTag == typeTag[Vector[Double]] =>
        val rdd = a.rdd.map { case (id, v) => Row(id, v) }
        val schema = StructType(Seq(
          StructField("id", LongType, false),
          StructField("value", ArrayType(DoubleType, false), false)))
        writeRDD(rdd, schema, e)
      case s: ScalarData[_] => {
        val format = TypeTagToFormat.typeTagToFormat(s.typeTag)
        val jsonString = Json.stringify(format.writes(s.value))
        val dir = new File(getGUIDPath(e))
        if (!dir.exists()) dir.mkdir()
        this match {
          case dst: UnorderedSphynxSparkDisk =>
            val fname = (dst.dataDir / s.gUID.toString / "serialized_data")
            val successFile = (dst.dataDir / s.gUID.toString / "_SUCCESS")
            fname.createFromStrings(jsonString)
            successFile.create()
          case dst: UnorderedSphynxLocalDisk =>
            val fname = s"${dst.getGUIDPath(e)}/serialized_data"
            val successFile = s"${dst.getGUIDPath(e)}/_SUCCESS"
            val file = new File(fname)
            val bw = new BufferedWriter(new FileWriter(file))
            bw.write(jsonString)
            bw.close()
            new File(successFile).createNewFile()
        }
      }
      case e => throw new AssertionError(s"Relocation not implemented for $e")
    }
  }
}

class UnorderedSphynxLocalDisk(host: String, port: Int, certDir: String, val dataDir: String)
  extends UnorderedSphynxDisk(host, port, certDir) {

  override def has(entity: MetaGraphEntity): Boolean = {
    new java.io.File(s"${dataDir}/${entity.gUID.toString}/_SUCCESS").exists()
  }

  override def canRelocateFrom(source: Domain): Boolean = {
    source match {
      case _: SphynxMemory => true
      case source: SparkDomain => source.isLocal
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
          case s: Scalar[_] => client.writeToUnorderedDisk(s)
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        }
      }
      case source: SparkDomain => relocateFromSpark(e, source)
      case source: UnorderedSphynxSparkDisk => {
        SafeFuture.async({
          val srcDir = source.dataDir / e.gUID.toString
          val srcFiles = (srcDir / "part-*").list
          val dstDir = s"${dataDir}/${e.gUID.toString}"
          try {
            for (f <- srcFiles) {
              f.copyToLocalFile(s"${dstDir}/${f.name}")
            }
          } catch {
            case t: Throwable => throw new AssertionError(s"Failed to relocate $e from $source", t)
          }
          new File(s"${dstDir}/_SUCCESS").createNewFile()
        })
      }
      case _ => throw new AssertionError(s"Cannot fetch $e from $source")
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
  override def has(e: MetaGraphEntity): Boolean = {
    (dataDir / e.gUID.toString / "_SUCCESS").exists()
  }

  override def getGUIDPath(e: MetaGraphEntity) = {
    (dataDir / e.gUID.toString).resolvedName
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    source match {
      case source: UnorderedSphynxLocalDisk => SafeFuture.async({
        val dstDir = dataDir / e.gUID.toString
        val srcFiles: Seq[File] = e match {
          case s: Scalar[_] =>
            Seq(new File(s"${source.getGUIDPath(s)}/serialized_data"))
          case _ =>
            val srcDir = new File(source.getGUIDPath(e))
            srcDir.listFiles.filter(_.getName.startsWith("part-"))
        }
        try {
          for (f <- srcFiles) {
            (dstDir / f.getName()).copyFromLocalFile(f.getPath())
          }
        } catch {
          case t: Throwable => throw new AssertionError(s"Failed to relocate $e from $source", t)
        }
        (dstDir / "_SUCCESS").create()
      })
      case source: SparkDomain => relocateFromSpark(e, source)
    }
  }
}
