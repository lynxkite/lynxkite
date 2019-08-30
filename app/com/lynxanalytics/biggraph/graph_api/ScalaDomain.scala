// The ScalaDomain can run local Scala operations.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

import org.apache.spark
import org.apache.spark.sql.SQLContext
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api.io.DataRoot
import com.lynxanalytics.biggraph.graph_api.io.EntityIO
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.ControlledFutures
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

class ScalaDomain extends Domain {
  private val entityCache = TrieMap[UUID, Any]()

  private def set(entity: MetaGraphEntity, data: Any): Unit = synchronized {
    entityCache(entity.gUID) = data
  }
  override def has(e: MetaGraphEntity): Boolean = synchronized {
    entityCache.contains(e.gUID)
  }
  override def compute(e: MetaGraphEntity): SafeFuture[Unit] = SafeFuture.successful(())
  override def cache(e: MetaGraphEntity): Unit = ()
  override def get[T](e: Scalar[T]): SafeFuture[T] = synchronized {
    SafeFuture.successful {
      entityCache(e.gUID).asInstanceOf[T]
    }
  }
  override def canCompute(e: MetaGraphEntity): Boolean = false

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
