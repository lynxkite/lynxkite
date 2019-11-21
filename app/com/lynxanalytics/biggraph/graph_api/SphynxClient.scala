package com.lynxanalytics.biggraph.graph_api

import _root_.io.grpc.netty.NettyChannelBuilder
import _root_.io.grpc.netty.GrpcSslContexts
import _root_.io.grpc.StatusRuntimeException
import _root_.io.grpc.ManagedChannelBuilder
import com.lynxanalytics.biggraph.graph_api.proto._
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import java.io.File

class SphynxClient(host: String, port: Int, certDir: String) {

  private val channel = NettyChannelBuilder.forAddress(host, port)
    .sslContext(GrpcSslContexts.forClient().trustManager(new File(s"$certDir/cert.pem")).build())
    .build();

  private val blockingStub = SphynxGrpc.newBlockingStub(channel)

  def canCompute(operationMetadataJSON: String): Boolean = {
    val request = SphynxOuterClass.CanComputeRequest.newBuilder().setOperation(operationMetadataJSON).build()
    val response = blockingStub.canCompute(request)
    return response.getCanCompute
  }
}
