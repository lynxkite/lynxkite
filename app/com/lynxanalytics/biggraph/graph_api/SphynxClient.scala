package com.lynxanalytics.biggraph.graph_api

import _root_.io.grpc.netty.NettyChannelBuilder
import _root_.io.grpc.netty.GrpcSslContexts
import _root_.io.grpc.StatusRuntimeException
import _root_.io.grpc.ManagedChannelBuilder
import com.lynxanalytics.biggraph.graph_api.proto._
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import java.io.File

class SphynxClient(host: String, port: Int) {

  private val channel = {
    val cert_dir = LoggedEnvironment.envOrNone("SPHYNX_CERT_DIR")
    cert_dir match {
      case Some(cert_dir) => NettyChannelBuilder.forAddress(host, port)
        .sslContext(GrpcSslContexts.forClient().trustManager(new File(s"$cert_dir/cert.pem")).build())
        .build();
      case None => {
        println("Using unsecure channel to communicate with Sphynx.")
        ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
      }
    }
  }

  private val blockingStub = SphynxGrpc.newBlockingStub(channel)

  def canCompute(operationMetadataJSON: String): Boolean = {
    val request = SphynxOuterClass.CanComputeRequest.newBuilder().setOperation(operationMetadataJSON).build()
    val response = blockingStub.canCompute(request)
    println("CanCompute: " + response.getCanCompute)
    return response.getCanCompute
  }
}
