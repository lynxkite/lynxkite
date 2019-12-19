package com.lynxanalytics.biggraph.graph_api.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.24.0)",
    comments = "Source: sphynx.proto")
public final class SphynxGrpc {

  private SphynxGrpc() {}

  public static final String SERVICE_NAME = "sphynx.Sphynx";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply> getCanComputeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CanCompute",
      requestType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest.class,
      responseType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply> getCanComputeMethod() {
    io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply> getCanComputeMethod;
    if ((getCanComputeMethod = SphynxGrpc.getCanComputeMethod) == null) {
      synchronized (SphynxGrpc.class) {
        if ((getCanComputeMethod = SphynxGrpc.getCanComputeMethod) == null) {
          SphynxGrpc.getCanComputeMethod = getCanComputeMethod =
              io.grpc.MethodDescriptor.<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CanCompute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply.getDefaultInstance()))
              .setSchemaDescriptor(new SphynxMethodDescriptorSupplier("CanCompute"))
              .build();
        }
      }
    }
    return getCanComputeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply> getComputeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Compute",
      requestType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest.class,
      responseType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply> getComputeMethod() {
    io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply> getComputeMethod;
    if ((getComputeMethod = SphynxGrpc.getComputeMethod) == null) {
      synchronized (SphynxGrpc.class) {
        if ((getComputeMethod = SphynxGrpc.getComputeMethod) == null) {
          SphynxGrpc.getComputeMethod = getComputeMethod =
              io.grpc.MethodDescriptor.<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Compute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply.getDefaultInstance()))
              .setSchemaDescriptor(new SphynxMethodDescriptorSupplier("Compute"))
              .build();
        }
      }
    }
    return getComputeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply> getGetScalarMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetScalar",
      requestType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest.class,
      responseType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply> getGetScalarMethod() {
    io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply> getGetScalarMethod;
    if ((getGetScalarMethod = SphynxGrpc.getGetScalarMethod) == null) {
      synchronized (SphynxGrpc.class) {
        if ((getGetScalarMethod = SphynxGrpc.getGetScalarMethod) == null) {
          SphynxGrpc.getGetScalarMethod = getGetScalarMethod =
              io.grpc.MethodDescriptor.<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetScalar"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply.getDefaultInstance()))
              .setSchemaDescriptor(new SphynxMethodDescriptorSupplier("GetScalar"))
              .build();
        }
      }
    }
    return getGetScalarMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply> getWriteToUnorderedDiskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteToUnorderedDisk",
      requestType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest.class,
      responseType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply> getWriteToUnorderedDiskMethod() {
    io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply> getWriteToUnorderedDiskMethod;
    if ((getWriteToUnorderedDiskMethod = SphynxGrpc.getWriteToUnorderedDiskMethod) == null) {
      synchronized (SphynxGrpc.class) {
        if ((getWriteToUnorderedDiskMethod = SphynxGrpc.getWriteToUnorderedDiskMethod) == null) {
          SphynxGrpc.getWriteToUnorderedDiskMethod = getWriteToUnorderedDiskMethod =
              io.grpc.MethodDescriptor.<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteToUnorderedDisk"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply.getDefaultInstance()))
              .setSchemaDescriptor(new SphynxMethodDescriptorSupplier("WriteToUnorderedDisk"))
              .build();
        }
      }
    }
    return getWriteToUnorderedDiskMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply> getRelocateFromSphynxDiskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RelocateFromSphynxDisk",
      requestType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest.class,
      responseType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply> getRelocateFromSphynxDiskMethod() {
    io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply> getRelocateFromSphynxDiskMethod;
    if ((getRelocateFromSphynxDiskMethod = SphynxGrpc.getRelocateFromSphynxDiskMethod) == null) {
      synchronized (SphynxGrpc.class) {
        if ((getRelocateFromSphynxDiskMethod = SphynxGrpc.getRelocateFromSphynxDiskMethod) == null) {
          SphynxGrpc.getRelocateFromSphynxDiskMethod = getRelocateFromSphynxDiskMethod =
              io.grpc.MethodDescriptor.<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RelocateFromSphynxDisk"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply.getDefaultInstance()))
              .setSchemaDescriptor(new SphynxMethodDescriptorSupplier("RelocateFromSphynxDisk"))
              .build();
        }
      }
    }
    return getRelocateFromSphynxDiskMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply> getHasOnSphynxDiskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HasOnSphynxDisk",
      requestType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest.class,
      responseType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply> getHasOnSphynxDiskMethod() {
    io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply> getHasOnSphynxDiskMethod;
    if ((getHasOnSphynxDiskMethod = SphynxGrpc.getHasOnSphynxDiskMethod) == null) {
      synchronized (SphynxGrpc.class) {
        if ((getHasOnSphynxDiskMethod = SphynxGrpc.getHasOnSphynxDiskMethod) == null) {
          SphynxGrpc.getHasOnSphynxDiskMethod = getHasOnSphynxDiskMethod =
              io.grpc.MethodDescriptor.<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HasOnSphynxDisk"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply.getDefaultInstance()))
              .setSchemaDescriptor(new SphynxMethodDescriptorSupplier("HasOnSphynxDisk"))
              .build();
        }
      }
    }
    return getHasOnSphynxDiskMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply> getHasInSphynxMemoryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HasInSphynxMemory",
      requestType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest.class,
      responseType = com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest,
      com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply> getHasInSphynxMemoryMethod() {
    io.grpc.MethodDescriptor<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply> getHasInSphynxMemoryMethod;
    if ((getHasInSphynxMemoryMethod = SphynxGrpc.getHasInSphynxMemoryMethod) == null) {
      synchronized (SphynxGrpc.class) {
        if ((getHasInSphynxMemoryMethod = SphynxGrpc.getHasInSphynxMemoryMethod) == null) {
          SphynxGrpc.getHasInSphynxMemoryMethod = getHasInSphynxMemoryMethod =
              io.grpc.MethodDescriptor.<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest, com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HasInSphynxMemory"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply.getDefaultInstance()))
              .setSchemaDescriptor(new SphynxMethodDescriptorSupplier("HasInSphynxMemory"))
              .build();
        }
      }
    }
    return getHasInSphynxMemoryMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SphynxStub newStub(io.grpc.Channel channel) {
    return new SphynxStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SphynxBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new SphynxBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SphynxFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new SphynxFutureStub(channel);
  }

  /**
   */
  public static abstract class SphynxImplBase implements io.grpc.BindableService {

    /**
     */
    public void canCompute(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply> responseObserver) {
      asyncUnimplementedUnaryCall(getCanComputeMethod(), responseObserver);
    }

    /**
     */
    public void compute(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply> responseObserver) {
      asyncUnimplementedUnaryCall(getComputeMethod(), responseObserver);
    }

    /**
     */
    public void getScalar(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply> responseObserver) {
      asyncUnimplementedUnaryCall(getGetScalarMethod(), responseObserver);
    }

    /**
     */
    public void writeToUnorderedDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply> responseObserver) {
      asyncUnimplementedUnaryCall(getWriteToUnorderedDiskMethod(), responseObserver);
    }

    /**
     */
    public void relocateFromSphynxDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply> responseObserver) {
      asyncUnimplementedUnaryCall(getRelocateFromSphynxDiskMethod(), responseObserver);
    }

    /**
     */
    public void hasOnSphynxDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply> responseObserver) {
      asyncUnimplementedUnaryCall(getHasOnSphynxDiskMethod(), responseObserver);
    }

    /**
     */
    public void hasInSphynxMemory(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply> responseObserver) {
      asyncUnimplementedUnaryCall(getHasInSphynxMemoryMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCanComputeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply>(
                  this, METHODID_CAN_COMPUTE)))
          .addMethod(
            getComputeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply>(
                  this, METHODID_COMPUTE)))
          .addMethod(
            getGetScalarMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply>(
                  this, METHODID_GET_SCALAR)))
          .addMethod(
            getWriteToUnorderedDiskMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply>(
                  this, METHODID_WRITE_TO_UNORDERED_DISK)))
          .addMethod(
            getRelocateFromSphynxDiskMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply>(
                  this, METHODID_RELOCATE_FROM_SPHYNX_DISK)))
          .addMethod(
            getHasOnSphynxDiskMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply>(
                  this, METHODID_HAS_ON_SPHYNX_DISK)))
          .addMethod(
            getHasInSphynxMemoryMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply>(
                  this, METHODID_HAS_IN_SPHYNX_MEMORY)))
          .build();
    }
  }

  /**
   */
  public static final class SphynxStub extends io.grpc.stub.AbstractStub<SphynxStub> {
    private SphynxStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SphynxStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SphynxStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SphynxStub(channel, callOptions);
    }

    /**
     */
    public void canCompute(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCanComputeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void compute(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getComputeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getScalar(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetScalarMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void writeToUnorderedDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getWriteToUnorderedDiskMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void relocateFromSphynxDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRelocateFromSphynxDiskMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void hasOnSphynxDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHasOnSphynxDiskMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void hasInSphynxMemory(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest request,
        io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHasInSphynxMemoryMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class SphynxBlockingStub extends io.grpc.stub.AbstractStub<SphynxBlockingStub> {
    private SphynxBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SphynxBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SphynxBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SphynxBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply canCompute(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest request) {
      return blockingUnaryCall(
          getChannel(), getCanComputeMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply compute(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest request) {
      return blockingUnaryCall(
          getChannel(), getComputeMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply getScalar(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetScalarMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply writeToUnorderedDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest request) {
      return blockingUnaryCall(
          getChannel(), getWriteToUnorderedDiskMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply relocateFromSphynxDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest request) {
      return blockingUnaryCall(
          getChannel(), getRelocateFromSphynxDiskMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply hasOnSphynxDisk(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest request) {
      return blockingUnaryCall(
          getChannel(), getHasOnSphynxDiskMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply hasInSphynxMemory(com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest request) {
      return blockingUnaryCall(
          getChannel(), getHasInSphynxMemoryMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class SphynxFutureStub extends io.grpc.stub.AbstractStub<SphynxFutureStub> {
    private SphynxFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SphynxFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SphynxFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SphynxFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply> canCompute(
        com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCanComputeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply> compute(
        com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getComputeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply> getScalar(
        com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetScalarMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply> writeToUnorderedDisk(
        com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getWriteToUnorderedDiskMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply> relocateFromSphynxDisk(
        com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRelocateFromSphynxDiskMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply> hasOnSphynxDisk(
        com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getHasOnSphynxDiskMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply> hasInSphynxMemory(
        com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getHasInSphynxMemoryMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CAN_COMPUTE = 0;
  private static final int METHODID_COMPUTE = 1;
  private static final int METHODID_GET_SCALAR = 2;
  private static final int METHODID_WRITE_TO_UNORDERED_DISK = 3;
  private static final int METHODID_RELOCATE_FROM_SPHYNX_DISK = 4;
  private static final int METHODID_HAS_ON_SPHYNX_DISK = 5;
  private static final int METHODID_HAS_IN_SPHYNX_MEMORY = 6;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SphynxImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(SphynxImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CAN_COMPUTE:
          serviceImpl.canCompute((com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest) request,
              (io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply>) responseObserver);
          break;
        case METHODID_COMPUTE:
          serviceImpl.compute((com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeRequest) request,
              (io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.ComputeReply>) responseObserver);
          break;
        case METHODID_GET_SCALAR:
          serviceImpl.getScalar((com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarRequest) request,
              (io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.GetScalarReply>) responseObserver);
          break;
        case METHODID_WRITE_TO_UNORDERED_DISK:
          serviceImpl.writeToUnorderedDisk((com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskRequest) request,
              (io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.WriteToUnorderedDiskReply>) responseObserver);
          break;
        case METHODID_RELOCATE_FROM_SPHYNX_DISK:
          serviceImpl.relocateFromSphynxDisk((com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskRequest) request,
              (io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.RelocateFromSphynxDiskReply>) responseObserver);
          break;
        case METHODID_HAS_ON_SPHYNX_DISK:
          serviceImpl.hasOnSphynxDisk((com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskRequest) request,
              (io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasOnSphynxDiskReply>) responseObserver);
          break;
        case METHODID_HAS_IN_SPHYNX_MEMORY:
          serviceImpl.hasInSphynxMemory((com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryRequest) request,
              (io.grpc.stub.StreamObserver<com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.HasInSphynxMemoryReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class SphynxBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SphynxBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Sphynx");
    }
  }

  private static final class SphynxFileDescriptorSupplier
      extends SphynxBaseDescriptorSupplier {
    SphynxFileDescriptorSupplier() {}
  }

  private static final class SphynxMethodDescriptorSupplier
      extends SphynxBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    SphynxMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (SphynxGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SphynxFileDescriptorSupplier())
              .addMethod(getCanComputeMethod())
              .addMethod(getComputeMethod())
              .addMethod(getGetScalarMethod())
              .addMethod(getWriteToUnorderedDiskMethod())
              .addMethod(getRelocateFromSphynxDiskMethod())
              .addMethod(getHasOnSphynxDiskMethod())
              .addMethod(getHasInSphynxMemoryMethod())
              .build();
        }
      }
    }
    return result;
  }
}
