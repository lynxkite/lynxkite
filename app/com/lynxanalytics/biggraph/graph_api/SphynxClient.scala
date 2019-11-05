package com.lynxanalytics.biggraph.graph_api

import _root_.io.grpc.netty.NettyChannelBuilder
import _root_.io.grpc.netty.GrpcSslContexts
import _root_.io.grpc.StatusRuntimeException
import _root_.io.grpc.ManagedChannelBuilder
import _root_.io.grpc.stub.StreamObserver
import com.lynxanalytics.biggraph.graph_api.proto._
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import java.io.File
import scala.reflect.runtime.universe._
import scala.concurrent.Promise
import scala.util.{ Success, Failure }

class SphynxClient(host: String, port: Int) {
  // Exchanges messages with Sphynx.

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
  private val asyncStub = SphynxGrpc.newStub(channel)

  def canCompute(operationMetadataJSON: String): Boolean = {
    val request = SphynxOuterClass.CanComputeRequest.newBuilder().setOperation(operationMetadataJSON).build()
    val response = blockingStub.canCompute(request)
    println("CanCompute: " + response.getCanCompute)
    return response.getCanCompute
  }

  def compute(operationMetadataJSON: String): Promise[Unit] = {
    val request = SphynxOuterClass.ComputeRequest.newBuilder().setOperation(operationMetadataJSON).build()
    println("Computation started.")
    val p = Promise[Unit]()
    var computed = false
    asyncStub.compute(request, new StreamObserver[SphynxOuterClass.ComputeReply] {
      def onNext(r: SphynxOuterClass.ComputeReply) {
        if (computed) {
          println(s"$operationMetadataJSON was computed twice!")
        }
        computed = true
      }
      def onError(t: Throwable) {
        p.complete(Failure(t))
      }
      def onCompleted() {
        if (computed) {
          p.complete(Success(()))
        } else {
          val e = new Exception(f"$operationMetadataJSON was not computed.")
          p.complete(Failure(e))
        }
      }
    })
    p.future
  }

  def getScalar(gUID: String): String = {
    val request = SphynxOuterClass.GetScalarRequest.newBuilder().setGuid(gUID).build()
    val response = blockingStub.getScalar(request)
    println("GetScalar called.")
    response.getScalar
  }
}
