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
  private val entityCache = TrieMap[UUID, SafeFuture[Any]]()

  private def set(entity: MetaGraphEntity, data: SafeFuture[Any]) = synchronized {
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

  override def getProgress(entity: MetaGraphEntity): Double = synchronized {
    val guid = entity.gUID
    if (entityCache.contains(guid)) {
      entityCache(guid).value match {
        case None => 0.5
        case Some(Failure(_)) => -1.0
        case Some(Success(_)) => 1.0
      }
    } else 0.0
  }
}
