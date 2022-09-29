package com.lynxanalytics.lynxkite.graph_api

import _root_.io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import _root_.io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import _root_.io.grpc.StatusRuntimeException
import _root_.io.grpc.stub.StreamObserver
import com.lynxanalytics.lynxkite.graph_api.proto._
import com.lynxanalytics.lynxkite.Environment
import java.io.File
import scala.reflect.runtime.universe._
import scala.concurrent.{Promise, Future}
import scala.util.{Success, Failure}
import play.api.libs.json.Json
import scala.concurrent.ExecutionContext
import java.util.concurrent.TimeUnit

class SingleResponseStreamObserver[T] extends StreamObserver[T] {
  private val promise = Promise[T]()
  val future = SafeFuture.wrap(promise.future)
  var responseArrived = false
  def onNext(r: T) {
    assert(!responseArrived, s"Two responses arrived, while we expected only one.")
    responseArrived = true
    promise.complete(Success(r))
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

class SphynxClient(host: String, port: Int, certDir: String)(implicit ec: ExecutionContext) {
  // Exchanges messages with Sphynx.

  private def sslContext =
    GrpcSslContexts.forClient.trustManager(new File(s"$certDir/cert.pem")).build
  private def builder = NettyChannelBuilder.forAddress(host, port)
  private val channel =
    if (certDir.nonEmpty) builder.sslContext(sslContext).build
    else builder.usePlaintext.build
  private val blockingStub = SphynxGrpc.newBlockingStub(channel)
  private val asyncStub = SphynxGrpc.newStub(channel)

  def canCompute(operationMetadataJSON: String, domain: String): Boolean = {
    val request = SphynxOuterClass.CanComputeRequest.newBuilder()
      .setOperation(operationMetadataJSON).setDomain(domain).build()
    val response = blockingStub.canCompute(request)
    response.getCanCompute
  }

  def compute(operationMetadataJSON: String, domain: String): SafeFuture[Unit] = {
    val request = SphynxOuterClass.ComputeRequest.newBuilder()
      .setOperation(operationMetadataJSON).setDomain(domain).build()
    val obs = new SingleResponseStreamObserver[SphynxOuterClass.ComputeReply]
    asyncStub.compute(request, obs)
    obs.future.map(_ => ())
  }

  def getScalar[T](scalar: Scalar[T]): SafeFuture[T] = {
    val gUIDString = scalar.gUID.toString()
    val request = SphynxOuterClass.GetScalarRequest.newBuilder().setGuid(gUIDString).build()
    val format = TypeTagToFormat.typeTagToFormat(scalar.typeTag)
    val obs = new SingleResponseStreamObserver[SphynxOuterClass.GetScalarReply]
    asyncStub.getScalar(request, obs)
    obs.future.map(r => format.reads(Json.parse(r.getScalar)).get)
  }

  def writeToUnorderedDisk(e: MetaGraphEntity): SafeFuture[Unit] = {
    // In SphynxMemory, vertices are indexed from 0 to n. This method asks Sphynx
    // to reindex vertices to use Spark-side indices and write the result into
    // a file on UnorderedSphynxDisk. For this, some entity types need extra vertex set guids

    val request = e match {
      case a: Attribute[_] =>
        SphynxOuterClass.WriteToUnorderedDiskRequest.newBuilder()
          .setGuid(e.gUID.toString)
          .setVsguid1(a.vertexSet.gUID.toString).build()
      case eb: EdgeBundle =>
        SphynxOuterClass.WriteToUnorderedDiskRequest.newBuilder()
          .setGuid(e.gUID.toString)
          .setVsguid1(eb.srcVertexSet.gUID.toString)
          .setVsguid2(eb.dstVertexSet.gUID.toString).build()
      case _ =>
        SphynxOuterClass.WriteToUnorderedDiskRequest.newBuilder()
          .setGuid(e.gUID.toString).build()
    }
    val obs = new SingleResponseStreamObserver[SphynxOuterClass.WriteToUnorderedDiskReply]
    asyncStub.writeToUnorderedDisk(request, obs)
    obs.future.map(_ => ())
  }

  def readFromUnorderedDisk(e: MetaGraphEntity): SafeFuture[Unit] = {
    // Asks Sphynx to read the data from UnorderedSphynxDisk and reindex the
    // vertices to use indices from 0 to n.
    val guid = e.gUID.toString()
    val requestBuilder = SphynxOuterClass.ReadFromUnorderedDiskRequest.newBuilder()
      .setGuid(e.gUID.toString)
      .setType(e.typeString)
    val request = e match {
      case v: VertexSet => requestBuilder.build()
      case eb: EdgeBundle => requestBuilder
          .setVsguid1(eb.srcVertexSet.gUID.toString)
          .setVsguid2(eb.dstVertexSet.gUID.toString).build()
      case a: Attribute[_] => requestBuilder
          .setAttributeType(a.typeTag.toString)
          .setVsguid1(a.vertexSet.gUID.toString).build()
      case s: Scalar[_] => requestBuilder.build()
      case _ => ???
    }
    val obs = new SingleResponseStreamObserver[SphynxOuterClass.ReadFromUnorderedDiskReply]
    asyncStub.readFromUnorderedDisk(request, obs)
    obs.future.map(_ => ())
  }

  def hasOnOrderedSphynxDisk(e: MetaGraphEntity): Boolean = {
    val request = SphynxOuterClass.HasOnOrderedSphynxDiskRequest.newBuilder().setGuid(e.gUID.toString).build()
    val response = blockingStub.hasOnOrderedSphynxDisk(request)
    response.getHasOnDisk
  }

  def hasInSphynxMemory(e: MetaGraphEntity): Boolean = {
    val request = SphynxOuterClass.HasInSphynxMemoryRequest.newBuilder().setGuid(e.gUID.toString).build()
    val response = blockingStub.hasInSphynxMemory(request)
    response.getHasInMemory
  }

  def readFromOrderedSphynxDisk(e: MetaGraphEntity): SafeFuture[Unit] = {
    val guid = e.gUID.toString()
    val request = SphynxOuterClass.ReadFromOrderedSphynxDiskRequest.newBuilder().setGuid(guid).build()
    val obs = new SingleResponseStreamObserver[SphynxOuterClass.ReadFromOrderedSphynxDiskReply]
    asyncStub.readFromOrderedSphynxDisk(request, obs)
    obs.future.map(_ => ())
  }

  def writeToOrderedDisk(e: MetaGraphEntity): SafeFuture[Unit] = {
    val request = SphynxOuterClass.WriteToOrderedDiskRequest.newBuilder().setGuid(e.gUID.toString).build()
    val obs = new SingleResponseStreamObserver[SphynxOuterClass.WriteToOrderedDiskReply]
    asyncStub.writeToOrderedDisk(request, obs)
    obs.future.map(_ => ())
  }

  def clear(domain: String): SafeFuture[Unit] = {
    val request = SphynxOuterClass.ClearRequest.newBuilder().setDomain(domain).build()
    val obs = new SingleResponseStreamObserver[SphynxOuterClass.ClearReply]
    asyncStub.clear(request, obs)
    obs.future.map(_ => ())
  }

  def shutDownChannel = {
    val isShutdown = channel.shutdown.awaitTermination(30, TimeUnit.SECONDS)
    if (!isShutdown) channel.shutdownNow
  }

}
