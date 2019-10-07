package com.lynxanalytics.biggraph.graph_api

import _root_.io.grpc.ManagedChannel
import _root_.io.grpc.ManagedChannelBuilder
import _root_.io.grpc.StatusRuntimeException
import com.lynxanalytics.biggraph.graph_api.proto._;

object SphynxClient {
  def apply(host: String, port: Int): SphynxClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = SphynxGrpc.newBlockingStub(channel);
    new SphynxClient(channel, blockingStub)
  }
}

class SphynxClient private (
    private val channel: ManagedChannel,
    private val blockingStub: SphynxGrpc.SphynxBlockingStub) {

  def canCompute(operationMetadataJSON: String): Boolean = {
    val request = SphynxOuterClass.CanComputeRequest.newBuilder().setOperation(operationMetadataJSON).build()
    val response = blockingStub.canCompute(request)
    println("CanCompute: " + response.getCanCompute)
    return response.getCanCompute
  }
}
