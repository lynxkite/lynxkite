// The DataManager triggers the execution of computation for a MetaGraphEntity.
// It can schedule the execution on one of the domains it controls.
// It can return values for Scalars and information about execution status.

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment

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
  override def toString = this.getClass.getSimpleName // Looks better in debug prints.
  def has(e: MetaGraphEntity): Boolean
  def compute(op: MetaGraphOperationInstance): SafeFuture[Unit]
  // A hint that this entity is likely to be used repeatedly.
  def cache(e: MetaGraphEntity): Unit
  def get[T](e: Scalar[T]): SafeFuture[T]
  def canCompute(op: MetaGraphOperationInstance): Boolean
  // Moves an entity from another Domain to this one.
  // This is a method on the destination so that the methods for modifying internal
  // data structures can remain private.
  def relocate(e: MetaGraphEntity, source: Domain): SafeFuture[Unit]
}

// Manages data computation across domains.
class DataManager(
    // The domains are in order of preference.
    val domains: Seq[Domain]) extends EntityProgressManager {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "DataManager",
      maxParallelism = LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)
  private val futures =
    collection.concurrent.TrieMap[(java.util.UUID, Domain), SafeFuture[Unit]]()

  private def findFailure(fs: Iterable[SafeFuture[_]]): Option[Throwable] = {
    fs.map(_.value).collectFirst { case Some(util.Failure(t)) => t }
  }

  override def computeProgress(entity: MetaGraphEntity): Double = {
    val d = bestSource(entity)
    if (d.has(entity)) {
      futures((entity.gUID, d)) = SafeFuture.successful(())
    }
    futures.get((entity.gUID, d)) match {
      case None => 0.0
      case Some(s) =>
        if (s.hasFailed) -1.0
        else {
          val deps = s.dependencySet
          if (findFailure(deps).isDefined) -1.0
          else 1.0 / (1.0 + deps.size - deps.filter(_.isCompleted).size)
        }
    }
  }

  override def getComputedScalarValue[T](e: Scalar[T]): ScalarComputationState[T] = {
    computeProgress(e) match {
      case 1.0 => ScalarComputationState(1, Some(get(e)), None)
      case -1.0 => ScalarComputationState(-1, None, findFailure(getFuture(e).dependencySet))
      case x => ScalarComputationState(x, None, None)
    }
  }

  private def bestSource(e: MetaGraphEntity): Domain = {
    domains.find(_.has(e)) match {
      case Some(d) => d
      case None => domains.find(_.canCompute(e.source)).get
    }
  }

  def compute(entity: MetaGraphEntity): SafeFuture[Unit] = synchronized {
    ensure(entity, bestSource(entity))
  }

  def getFuture[T](scalar: Scalar[T]): SafeFuture[T] = synchronized {
    val d = bestSource(scalar)
    ensure(scalar, d).flatMap(_ => d.get(scalar))
  }

  def get[T](scalar: Scalar[T]): T = {
    await(getFuture(scalar))
  }

  private def relocate(e: MetaGraphEntity, src: Domain, dst: Domain): SafeFuture[Unit] = {
    e match {
      case e: Attribute[_] => ensure(e.vertexSet, dst).flatMap(_ => dst.relocate(e, src))
      case e: EdgeBundle => ensure(e.idSet, dst).flatMap(_ => dst.relocate(e, src))
      case _ => dst.relocate(e, src)
    }
  }

  def ensure(e: MetaGraphEntity, d: Domain): SafeFuture[Unit] = synchronized {
    val f = futures.get((e.gUID, d))
    if (f.isDefined && !f.get.hasFailed) {
      futures((e.gUID, d))
    } else {
      val other = bestSource(e)
      val f = if (d.has(e)) { // We have it. Great.
        SafeFuture.successful(())
      } else if (other.has(e)) { // Someone else has it. Relocate.
        relocate(e, other, d)
      } else if (d.canCompute(e.source)) { // Nobody has it, but we can compute. Compute.
        val f = ensureInputs(e, d).flatMap(_ => d.compute(e.source))
        for (o <- e.source.outputs.all.values) {
          futures((o.gUID, d)) = f
        }
        f
      } else { // Someone else has to compute it. Then we relocate.
        ensure(e, other).flatMap(_ => relocate(e, other, d))
      }
      futures((e.gUID, d)) = f
      f
    }
  }

  private def ensureInputs(e: MetaGraphEntity, d: Domain): SafeFuture[Unit] = {
    SafeFuture.sequence(
      e.source.inputs.all.values.map { input =>
        ensure(input, d)
      }).map(_ => ())
  }

  def cache(entity: MetaGraphEntity): Unit = {
    val d = bestSource(entity)
    ensure(entity, d).map(_ => d.cache(entity))
  }

  def waitAllFutures(): Unit = {
    SafeFuture.sequence(futures.values).awaitReady(concurrent.duration.Duration.Inf)
  }

  def clear(): Unit = synchronized {
    futures.clear()
  }

  // Convenience for awaiting something in this execution context.
  def await[T](f: SafeFuture[T]): T = f.awaitResult(concurrent.duration.Duration.Inf)
}
