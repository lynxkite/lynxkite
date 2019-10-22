package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util
import play.api.libs.json.Json

class SphynxMemory(host: String, port: Int) extends Domain {

  val client = new SphynxClient(host, port)

  override def has(entity: MetaGraphEntity): Boolean = {
    return false
  }

  override def compute(instance: MetaGraphOperationInstance): SafeFuture[Unit] = {
    ???
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    val json_meta = Json.stringify(MetaGraphManager.serializeOperation(instance))
    val res = client.canCompute(json_meta)
    println("Got a response from Sphynx!")
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
