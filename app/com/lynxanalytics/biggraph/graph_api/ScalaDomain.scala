// The ScalaDomain can run local Scala operations.

package com.lynxanalytics.biggraph.graph_api

import java.io.FileInputStream
import java.util.UUID

import collection.concurrent.TrieMap
import collection.JavaConverters._

import com.lynxanalytics.biggraph.graph_util
import proto.Entities

class ScalaDomain(val mixedDir: Option[String] = None) extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "ScalaDomain",
      maxParallelism = graph_util.LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)

  private val entityCache = TrieMap[UUID, Any]()

  private def set(entity: MetaGraphEntity, data: Any): Unit = synchronized {
    entityCache(entity.gUID) = data
  }
  override def has(e: MetaGraphEntity): Boolean = synchronized {
    entityCache.contains(e.gUID)
  }
  override def compute(instance: MetaGraphOperationInstance) = SafeFuture.async[Unit]({
    val op = instance.operation.asInstanceOf[ScalaOperation[_, _]]
    val outputs = collection.mutable.Map[Symbol, Any]()
    val inputs = synchronized {
      instance.inputs.all.mapValues(e => entityCache(e.gUID))
    }
    op.execute(inputs, outputs)
    synchronized {
      for ((symbol, data) <- outputs) {
        for (e <- instance.outputs.edgeBundles.get(symbol)) {
          if (e.autogenerateIdSet) {
            outputs(e.idSet.name) = data.asInstanceOf[Map[ID, Edge]].keySet
          }
        }
      }
      for ((symbol, e) <- instance.outputs.all) {
        entityCache(e.gUID) = outputs(symbol)
      }
    }
  })
  override def cache(e: MetaGraphEntity): Unit = ()
  override def get[T](e: Scalar[T]): SafeFuture[T] = synchronized {
    SafeFuture.successful(entityCache(e.gUID).asInstanceOf[T])
  }
  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    instance.operation.isInstanceOf[ScalaOperation[_, _]]
  }

  // Only use these for relocation.
  private[graph_api] def get(e: VertexSet) = synchronized { entityCache(e.gUID).asInstanceOf[Set[ID]] }
  private[graph_api] def get(e: EdgeBundle) = synchronized { entityCache(e.gUID).asInstanceOf[Map[ID, Edge]] }
  private[graph_api] def get[T](e: Attribute[T]) = synchronized { entityCache(e.gUID).asInstanceOf[Map[ID, T]] }

  override def canRelocate(source: Domain): Boolean = {
    source match {
      case source: SparkDomain => true
      case source: UnorderedSphynxDisk => true
      case _ => false
    }
  }

  override def relocate(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    source match {
      case source: SparkDomain =>
        val future = SafeFuture.async(source.getData(e) match {
          case v: VertexSetData => v.rdd.keys.collect.toSet
          case e: EdgeBundleData => e.rdd.collect.toMap
          case a: AttributeData[_] => a.rdd.collect.toMap
          case s: ScalarData[_] => s.value
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        })
        future.map(set(e, _))
      case source: UnorderedSphynxDisk => {
        val future = SafeFuture.async({
          val entityPath = s"${mixedDir.get}/${e.gUID.toString}"
          val file = new FileInputStream(entityPath)
          e match {
            case v: VertexSet => Entities.VertexSet.parseFrom(file)
              .getIdsList().asScala.toSet
            case e: EdgeBundle => Entities.EdgeBundle.parseFrom(file)
              .getEdgesList().asScala.map(e => (e.getId, Edge(e.getSrc, e.getDst))).toMap
            case a: Attribute[_] => ???
            case s: Scalar[_] => ???
            case e: HybridBundle => ???
            case t: Table => ???
          }
        })
        future.map(set(e, _))
      }
    }
  }

}

trait ScalaOperation[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider]
  extends TypedMetaGraphOp[IS, OMDS] {
  def execute(input: Map[Symbol, Any], output: collection.mutable.Map[Symbol, Any]): Unit
}
