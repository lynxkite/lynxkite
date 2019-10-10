package com.lynxanalytics.biggraph.graph_api

import _root_.io.grpc.ManagedChannel
import _root_.io.grpc.ManagedChannelBuilder
import _root_.io.grpc.StatusRuntimeException
import com.lynxanalytics.biggraph.graph_api.proto._

class SphynxClient(host: String, port: Int) {
  private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  private val blockingStub = SphynxGrpc.newBlockingStub(channel)

  def canCompute(operationMetadataJSON: String): Boolean = {
    val request = SphynxOuterClass.CanComputeRequest.newBuilder().setOperation(operationMetadataJSON).build()
    val response = blockingStub.canCompute(request)
    println("CanCompute: " + response.getCanCompute)
    return response.getCanCompute
  }
}
