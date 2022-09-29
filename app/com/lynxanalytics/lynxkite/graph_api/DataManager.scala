// The DataManager triggers the execution of computation for a MetaGraphEntity.
// It can schedule the execution on one of the domains it controls.
// It can return values for Scalars and information about execution status.

package com.lynxanalytics.lynxkite.graph_api

import com.lynxanalytics.lynxkite.graph_util.Environment

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
  // Computation happening for entities in the "ignore" set is ignored.
  def computeProgress(entities: Seq[MetaGraphEntity], ignore: Set[MetaGraphEntity] = Set()): Seq[Double]
  def computeProgress(entity: MetaGraphEntity): Double = computeProgress(Seq(entity)).head
  def getComputedScalarValue[T](entity: Scalar[T]): ScalarComputationState[T]
}

// Represents a data locality, such as "Spark" or "Scala" or "single-node server".
trait Domain {
  override def toString = this.getClass.getSimpleName // Looks better in debug prints.
  def has(e: MetaGraphEntity): Boolean
  def compute(op: MetaGraphOperationInstance): SafeFuture[Unit]
  def canCompute(op: MetaGraphOperationInstance): Boolean
  // A hint that this entity is likely to be used repeatedly.
  def cache(e: MetaGraphEntity): Unit
  def get[T](e: Scalar[T]): SafeFuture[T]
  def canGet[T](e: Scalar[T]): Boolean = true
  // Moves an entity from another Domain to this one.
  // This is a method on the destination so that the methods for modifying internal
  // data structures can remain private.
  def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit]
  def canRelocateFrom(source: Domain): Boolean
}

// Manages data computation across domains.
class DataManager(
    // The domains are in order of preference.
    val domains: Seq[Domain])
    extends EntityProgressManager {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "DataManager",
      maxParallelism = Environment.envOrElse("KITE_PARALLELISM", "5").toInt)
  private val futures =
    collection.mutable.Map[(java.util.UUID, Domain), SafeFuture[Unit]]()

  private def findFailure(fs: Iterable[SafeFuture[_]]): Option[Throwable] = {
    fs.map(_.value).collectFirst { case Some(util.Failure(t)) => t }
  }

  // This unique future is used to indicate that we have asked a domain
  // about an entity and they don't have it.
  private val DOES_NOT_HAVE = SafeFuture.successful(())

  override def computeProgress(
      entities: Seq[MetaGraphEntity],
      ignore: Set[MetaGraphEntity] = Set()): Seq[Double] = {
    // Returns all the futures that are responsible for producing the entity.
    def getFutures(e: MetaGraphEntity): Set[SafeFuture[_]] = synchronized {
      val sets = domains.map { d =>
        val set: Set[SafeFuture[_]] = futures.get((e.gUID, d)) match {
          case None =>
            val f = if (d.has(e)) SafeFuture.successful(()) else DOES_NOT_HAVE
            futures((e.gUID, d)) = f // Set the future, so next time we take the fast path.
            Set(f)
          case Some(s) =>
            if (s.hasFailed) Set(s)
            else s.dependencySet
        }
        set // Type inference fails otherwise.
      }
      sets.reduce(_.union(_))
    }
    try {
      val ignoredFutures: Set[SafeFuture[_]] = ignore.flatMap(getFutures(_))
      entities.map { entity =>
        val deps = getFutures(entity) - DOES_NOT_HAVE
        val done = deps.filter(f => f.isCompleted)
        val local = deps -- ignoredFutures
        val localDone = local.filter(f => f.isCompleted)
        val localComputing = local.filter(f => !f.isCompleted && !f.isWaiting)
        if (findFailure(deps).isDefined) -1.0
        else if (deps.size == 0) 0.0
        else if (done.size == deps.size) 1.0
        // Report as incomplete when nothing is running.
        else if (localComputing.size == 0) 0.0
        else (localDone.size + 0.5 * localComputing.size) / deps.size
      }
    } catch {
      case _: Throwable => Seq(0)
    }
  }

  override def getComputedScalarValue[T](e: Scalar[T]): ScalarComputationState[T] = {
    computeProgress(e) match {
      case 1.0 => ScalarComputationState(1, Some(get(e)), None)
      case -1.0 => ScalarComputationState(-1, None, findFailure(getFuture(e).dependencySet))
      case x => ScalarComputationState(x, None, None)
    }
  }

  private def whoHas(e: MetaGraphEntity): Option[Domain] = {
    domains.find(_.has(e))
  }

  private def whoCanCompute(e: MetaGraphEntity): Domain = {
    domains.find(_.canCompute(e.source)) match {
      case None => throw new AssertionError(f"None of the domains can compute $e.")
      case Some(d) => d
    }
  }

  def compute(e: MetaGraphEntity): SafeFuture[Unit] = synchronized {
    val d = whoHas(e).getOrElse(whoCanCompute(e))
    ensure(e, d)
  }

  def getFuture[T](scalar: Scalar[T]): SafeFuture[T] = synchronized {
    val d = whoHas(scalar).getOrElse(whoCanCompute(scalar))
    if (d.canGet(scalar)) {
      ensure(scalar, d).flatMap(_ => d.get(scalar))
    } else {
      val scalarDomain = domains.find(_.canGet(scalar)).get
      ensure(scalar, scalarDomain).flatMap(_ => scalarDomain.get(scalar))
    }
  }

  def get[T](scalar: Scalar[T]): T = {
    await(getFuture(scalar))
  }

  private def ensureThenRelocate(e: MetaGraphEntity, src: Domain, dst: Domain): SafeFuture[Unit] = {
    val directSrc = bfs(src, dst)
    val f = e match {
      // The base vertex set must be present for edges and attributes before we can relocate them.
      case e: Attribute[_] => combineFutures(Seq(ensure(e, directSrc), ensure(e.vertexSet, dst)))
      case e: EdgeBundle => combineFutures(Seq(
          ensure(e, directSrc),
          ensure(e.idSet, dst),
          ensure(e.srcVertexSet, dst),
          ensure(e.dstVertexSet, dst)))
      case _ => ensure(e, directSrc)
    }
    f.flatMap { _ =>
      val msg = s"RELOCATION_LOGGER_MARKER Moving ${e.gUID} from ${directSrc} to ${dst}"
      dst.relocateFrom(e, directSrc).withLogging(msg)
    }
  }

  private def bfs(src: Domain, dst: Domain): Domain = {
    // Uses bfs to determine the shortest path from src do dst and returns the
    // last Domain before dst on that path.
    val q = collection.mutable.Queue(src)
    val seen = collection.mutable.Set(src)
    while (!q.isEmpty) {
      var s = q.dequeue()
      for (d <- domains) {
        if (d == dst && d.canRelocateFrom(s)) {
          return s
        }
        if (!seen.contains(d) && d.canRelocateFrom(s)) {
          q.enqueue(d)
          seen += d
        }
      }
    }
    throw new AssertionError(f"Cannot relocate: no path was found from $src to $dst.")
  }

  def ensure(e: MetaGraphEntity, d: Domain): SafeFuture[Unit] = synchronized {
    val f = futures.get((e.gUID, d))
    if (f.isDefined) {
      if (f.get.hasFailed) { // Retry.
        futures((e.gUID, d)) = makeFuture(e, d)
      } else if (f.get.isCompleted) {
        if (d.has(e)) {
          futures((e.gUID, d)) = SafeFuture.successful(()) // Cut future chain.
        } else { // Domain has dropped it since then.
          futures((e.gUID, d)) = makeFuture(e, d)
        }
      } // Otherwise the computation is in progress and the existing future is good.
    } else {
      futures((e.gUID, d)) = makeFuture(e, d)
    }
    futures((e.gUID, d))
  }

  private val orderedSphynxDisk = domains.find(_.isInstanceOf[OrderedSphynxDisk])

  var kiteListener: com.lynxanalytics.lynxkite.controllers.KiteListener = null
  def setListener(listener: com.lynxanalytics.lynxkite.controllers.KiteListener) = synchronized {
    kiteListener = listener
  }

  private def makeFuture(e: MetaGraphEntity, d: Domain): SafeFuture[Unit] = synchronized {
    val source = whoHas(e).getOrElse(whoCanCompute(e))
    if (d.has(e)) { // We have it. Great.
      SafeFuture.successful(())
    } else if (source == d) { // Nobody has it, but this domain is the best to compute it. Compute.
      val f = ensureInputs(e, d).flatMap { _ =>
        e.source.inputs.all.map(_._2.gUID)
        val inputs = e.source.inputs.all.map(_._2.gUID).mkString(",")
        val outputs = e.source.outputs.all.map(_._2.gUID).mkString(",")
        val msg =
          s"OPERATION_LOGGER_MARKER $d opguid: ${e.source.gUID} inputs: |$inputs| outputs: |$outputs| op: ${e.source.operation}"
        val f = d.compute(e.source).withLogging(msg)
        if (kiteListener != null) {
          f.future.onComplete(_ => kiteListener.onDataManagerComputeCompleted())
        }
        f
      }
      if (f.hasFailed) {
        f.get // Cause the exception to be raised here.
      }

      for (o <- e.source.outputs.all.values) {
        futures((o.gUID, d)) = f
      }
      // After Sphynx computes something, we save it to ordered Sphynx disk. This makes it
      // possible to drop entities from Sphynx memory and load them from disk later.
      if (d.isInstanceOf[SphynxMemory]) {
        for (o <- e.source.outputs.all.values) {
          futures((o.gUID, orderedSphynxDisk.get)) = ensureThenRelocate(o, d, orderedSphynxDisk.get)
        }
      }
      f
    } else { // Someone else has it or will compute it. Then we relocate.
      ensureThenRelocate(e, source, d)
    }
  }

  private def ensureInputs(e: MetaGraphEntity, d: Domain): SafeFuture[Unit] = {
    val inputs = // Treat idsets of edgebundles as inputs. #8889
      e.source.inputs.all.values.toSet ++ e.source.inputs.edgeBundles.values.map(_.idSet).toSet
    combineFutures(inputs.map(ensure(_, d)))
  }

  private def combineFutures(fs: Iterable[SafeFuture[Unit]]): SafeFuture[Unit] = {
    SafeFuture.sequence(fs).map(_ => ())
  }

  def cache(e: MetaGraphEntity): Unit = {
    val d = whoHas(e).getOrElse(whoCanCompute(e))
    ensure(e, d).map(_ => d.cache(e))
  }

  def waitAllFutures(): Unit = {
    val f = synchronized { SafeFuture.sequence(futures.values) }
    f.awaitReady(concurrent.duration.Duration.Inf)
  }

  def clear(): Unit = synchronized {
    futures.clear()
  }

  // Convenience for awaiting something in this execution context.
  def await[T](f: SafeFuture[T]): T = f.awaitResult(concurrent.duration.Duration.Inf)
}
