// The DataManager triggers the execution of computation for a MetaGraphEntity.
// It can schedule the execution on one of the domains it controls.
// It can return ScalarData[T] values for Scalars.

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

trait EntityProgressManager {
  case class ScalarComputationState[T](
      computeProgress: Double,
      value: Option[T],
      error: Option[Throwable])
  // Returns an indication of whether the entity has already been computed.
  // 0 means it is not computed.
  // 1 means it is computed.
  // Anything in between indicates that the computation is in progress.
  // -1.0 indicates that an error has occurred during computation.
  // These constants need to be kept in sync with the ones in:
  // /web/app/script/util.js
  def computeProgress(entity: MetaGraphEntity): Double
  def getComputedScalarValue[T](entity: Scalar[T]): ScalarComputationState[T]
}

// Represents a data locality, such as "Spark" or "Scala" or "single-node server".
trait Domain {
  def has(e: MetaGraphEntity): Boolean
  // Returns the progress (0 - 1) or throws Exception if something is wrong.
  def getProgress(e: MetaGraphEntity): Double
  def compute(e: MetaGraphEntity): SafeFuture[Unit]
  // A hint that this entity is likely to be used repeatedly.
  def cache(e: MetaGraphEntity): Unit
  def get[T](e: Scalar[T]): SafeFuture[T]
  def canCompute(e: MetaGraphEntity): Boolean
}

// Manages data computation across domains.
class DataManager(
    // The domains are in order of preference.
    val domains: Seq[Domain]) extends EntityProgressManager {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "DataManager",
      maxParallelism = LoggedEnvironment.envOrElse("KITE_SPARK_PARALLELISM", "5").toInt)

  // This can be switched to false to enter "demo mode" where no new calculations are allowed.
  var computationAllowed = true

  override def computeProgress(entity: MetaGraphEntity): Double = {
    1
  }

  override def getComputedScalarValue[T](entity: Scalar[T]): ScalarComputationState[T] = {
    ScalarComputationState(1, None, None)
  }

  private def bestSource(e: MetaGraphEntity): Domain = {
    domains.find(_.has(e)) match {
      case Some(d) => d
      case None => domains.find(_.canCompute(e)).get
    }
  }

  def compute(entity: MetaGraphEntity): SafeFuture[Unit] = {
    ensure(entity, bestSource(entity))
  }

  def getFuture[T](scalar: Scalar[T]): SafeFuture[T] = {
    val d = bestSource(scalar)
    ensure(scalar, d).flatMap(_ => d.get(scalar))
  }

  def get[T](scalar: Scalar[T]): T = {
    getFuture(scalar).awaitResult(concurrent.duration.Duration.Inf)
  }

  private def ensure(e: MetaGraphEntity, d: Domain): SafeFuture[Unit] = {
    if (d.has(e)) {
      SafeFuture.successful(())
    } else if (computationAllowed) {
      if (d.canCompute(e)) {
        ensureInputs(e, d).map { _ =>
          d.compute(e)
        }
      } else {
        val other = bestSource(e)
        ensure(e, other)
        relocate(e, other, d)
      }
    } else SafeFuture.successful(())
  }

  private def ensureInputs(e: MetaGraphEntity, d: Domain): SafeFuture[Unit] = {
    SafeFuture.sequence {
      e.source.inputs.all.values.map { input =>
        ensure(input, d)
      }
    }.map(_ => ())
  }

  private def relocate(e: MetaGraphEntity, d1: Domain, d2: Domain): SafeFuture[Unit] = {
    SafeFuture.successful(())
  }

  def cache(entity: MetaGraphEntity): Unit = {
    bestSource(entity).cache(entity)
  }
}
