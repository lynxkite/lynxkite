// The SphynxDomain can connect to a Sphynx server that runs single node operations.

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util

class SphynxMemory(host: String, port: Int, certDir: String) extends Domain {

  val client = new SphynxClient(host, port, certDir)

  override def has(entity: MetaGraphEntity): Boolean = {
    return false
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    ???
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    val res = client.canCompute("Fake Operation Metadata in JSON")
    return res
  }

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = {
    ???
  }

  override def cache(e: MetaGraphEntity): Unit = {
    ???
  }

  override def relocate(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    ???
  }

}
