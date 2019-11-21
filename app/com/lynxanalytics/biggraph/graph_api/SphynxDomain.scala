// The SphynxDomain can connect to a Sphynx server that runs single-node operations.

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util
import play.api.libs.json.Json
import scala.reflect.runtime.universe.typeTag

class SphynxMemory(host: String, port: Int, certDir: String) extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "SphynxMemory",
      maxParallelism = graph_util.LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)

  val client = new SphynxClient(host, port, certDir)

  override def has(entity: MetaGraphEntity): Boolean = {
    return false
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

  override def canRelocate(source: Domain): Boolean = {
    source match {
      case source: UnorderedSphynxDisk => true
      case _ => false
    }
  }

  override def relocate(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    ???
  }

}

class UnorderedSphynxDisk(host: String, port: Int, certDir: String) extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "UnorderedSphynxDisk",
      maxParallelism = graph_util.LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)

  val client = new SphynxClient(host, port, certDir)

  override def has(entity: MetaGraphEntity): Boolean = {
    return false
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    ???
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    false
  }

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = {
    ???
  }

  override def cache(e: MetaGraphEntity): Unit = {
    ???
  }

  override def canRelocate(source: Domain): Boolean = {
    source match {
      case source: SphynxMemory => true
      case _ => false
    }
  }

  override def relocate(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    source match {
      case source: ScalaDomain => {
        ???
      }
      case source: SphynxMemory => {
        e match {
          case v: VertexSet => client.toRandomIndices(e.gUID.toString())
          case e: EdgeBundle => ???
          case a: Attribute[_] => ???
          case s: Scalar[_] => ???
          case e: HybridBundle => ???
          case t: Table => ???
        }
      }
    }
  }
}
