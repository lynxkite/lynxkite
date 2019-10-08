package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util

class SphynxMemory() extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "SphynxDomain",
      maxParallelism = graph_util.LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)

  val client = SphynxClient("localhost", 50051)

  override def has(entity: MetaGraphEntity): Boolean = {
    return false
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    ???
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    val res = client.canCompute("Fake Operation Metadata in JSON")
    println("Got a response from Sphynx!")
    return res
  }

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = {
    val e = new Exception("Sphynx is lazy now, won't get you any scalars.")
    ???
  }

  override def cache(e: MetaGraphEntity): Unit = ???
  }

  override def relocate(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    ???
  }

}
