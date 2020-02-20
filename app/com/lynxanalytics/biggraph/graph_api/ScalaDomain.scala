// The ScalaDomain can run local Scala operations.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import collection.concurrent.TrieMap

import com.lynxanalytics.biggraph.graph_util

class ScalaDomain extends Domain {
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

  override def canRelocateFrom(source: Domain): Boolean = {
    source match {
      case source: SparkDomain => true
      case _ => false
    }
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    val future = source match {
      case source: SparkDomain =>
        SafeFuture.async(source.getData(e) match {
          case v: VertexSetData => v.rdd.keys.collect.toSet
          case e: EdgeBundleData => e.rdd.collect.toMap
          case a: AttributeData[_] => a.rdd.collect.toMap
          case s: ScalarData[_] => s.value
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        })
    }
    future.map(set(e, _))
  }

}

trait ScalaOperation[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider]
  extends TypedMetaGraphOp[IS, OMDS] {
  def execute(input: Map[Symbol, Any], output: collection.mutable.Map[Symbol, Any]): Unit
}
