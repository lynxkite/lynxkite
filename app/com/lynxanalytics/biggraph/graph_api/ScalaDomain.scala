// The ScalaDomain can run local Scala operations.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

import scala.collection.concurrent.TrieMap

class ScalaDomain extends Domain {
  private val entityCache = TrieMap[UUID, Any]()

  private def set(entity: MetaGraphEntity, data: Any): Unit = synchronized {
    entityCache(entity.gUID) = data
  }
  override def has(e: MetaGraphEntity): Boolean = synchronized {
    entityCache.contains(e.gUID)
  }
  override def compute(e: MetaGraphEntity): SafeFuture[Unit] = SafeFuture.successful {
    val instance = e.source
    val op = instance.operation.asInstanceOf[ScalaOperation[_, _]]
    val outputs = collection.mutable.Map[Symbol, Any]()
    val inputs = instance.inputs.all.mapValues(e => entityCache(e.gUID))
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
  }
  override def cache(e: MetaGraphEntity): Unit = ()
  override def get[T](e: Scalar[T]): SafeFuture[T] = synchronized {
    SafeFuture.successful {
      entityCache(e.gUID).asInstanceOf[T]
    }
  }
  override def canCompute(e: MetaGraphEntity): Boolean = {
    e.source.operation.isInstanceOf[ScalaOperation[_, _]]
  }

  override def getProgress(e: MetaGraphEntity): Double = synchronized {
    if (entityCache.contains(e.gUID)) 1 else 0
  }

  def get(e: VertexSet) = synchronized { entityCache(e.gUID).asInstanceOf[Set[ID]] }
  def get(e: EdgeBundle) = synchronized { entityCache(e.gUID).asInstanceOf[Map[ID, Edge]] }
  def get[T](e: Attribute[T]) = synchronized { entityCache(e.gUID).asInstanceOf[Map[ID, T]] }

  override def relocate(e: MetaGraphEntity, source: Domain) = {
    source match {
      case source: SparkDomain =>
        implicit val ec = source.executionContext
        val future = source.getFuture(e).map {
          case v: VertexSetData => v.rdd.keys.collect.toSet
          case e: EdgeBundleData => e.rdd.collect.toMap
          case a: AttributeData[_] => a.rdd.collect.toMap
          case s: ScalarData[_] => s.value
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        }
        future.map(set(e, _))
    }
  }

}

trait ScalaOperation[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider]
  extends TypedMetaGraphOp[IS, OMDS] {
  def execute(input: Map[Symbol, Any], output: collection.mutable.Map[Symbol, Any]): Unit
}
