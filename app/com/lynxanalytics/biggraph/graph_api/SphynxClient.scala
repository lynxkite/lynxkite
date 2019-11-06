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
import scala.concurrent.{ Promise, Future }
import scala.util.{ Success, Failure }
import play.api.libs.json.Json

class SingleResponseStreamObserver[T, U](val handleReply: T => U) extends StreamObserver[T] {
  private val promise = Promise[U]()
  val future = SafeFuture(promise.future)
  var responseArrived = false
  def onNext(r: T) {
    assert(!responseArrived, s"Two responses arrived, while we expected only one.")
    responseArrived = true
    promise.complete(Success(handleReply(r)))
  }
  def onError(t: Throwable) {
    promise.complete(Failure(t))
  }
  def onCompleted() {
    if (!responseArrived) {
      val e = new Exception("No response arrived.")
      promise.complete(Failure(e))
    }
  }
}

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
    return response.getCanCompute
  }

  def compute(operationMetadataJSON: String): Future[Unit] = {
    val request = SphynxOuterClass.ComputeRequest.newBuilder().setOperation(operationMetadataJSON).build()
    val singleResponseStreamObserver = new SingleResponseStreamObserver[SphynxOuterClass.ComputeReply, Unit](_ => ())
    asyncStub.compute(request, singleResponseStreamObserver)
    singleResponseStreamObserver.promise.future
  }

  def getScalar[T](scalar: Scalar[T]): Future[T] = {
    val gUIDString = scalar.gUID.toString()
    val request = SphynxOuterClass.GetScalarRequest.newBuilder().setGuid(gUIDString).build()
    val singleResponseStreamObserver = new SingleResponseStreamObserver[SphynxOuterClass.GetScalarReply, T](r => {
      val jsonString = r.getScalar
      val format = TypeTagToFormat.typeTagToFormat(scalar.typeTag)
      format.reads(Json.parse(jsonString)).get
    })
    asyncStub.getScalar(request, singleResponseStreamObserver)
    singleResponseStreamObserver.promise.future
  }
}
