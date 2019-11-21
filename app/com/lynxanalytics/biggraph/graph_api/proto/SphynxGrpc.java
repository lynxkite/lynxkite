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

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCanComputeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeRequest,
                com.lynxanalytics.biggraph.graph_api.proto.SphynxOuterClass.CanComputeReply>(
                  this, METHODID_CAN_COMPUTE)))
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
  }

  private static final int METHODID_CAN_COMPUTE = 0;

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
              .build();
        }
      }
    }
    return result;
  }
}
