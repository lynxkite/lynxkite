#!/bin/bash -xue
# Generates Java and Go interfaces from proto files. These interfaces are used
# for LynxKite-Sphynx communication.

cd $(dirname $0)
. sphynx_common.sh

# Get protobuf compiler.
if ! type "protoc" > /dev/null; then
  PROTOC_VERSION="3.10.0"
  PROTOC_ZIP="protoc-$PROTOC_VERSION-linux-x86_64.zip"
  wget -nc https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP
  mkdir -p protoc
  unzip -n $PROTOC_ZIP bin/protoc -d protoc
fi

# Generate the gRPC Java interfaces.
GRPC_JAVA_VERSION="1.24.0"
GRPC_JAVA=protoc-gen-grpc-java-$GRPC_JAVA_VERSION-linux-x86_64.exe
wget -nc https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/$GRPC_JAVA_VERSION/$GRPC_JAVA
chmod +x $REPO/$GRPC_JAVA
protoc --plugin=protoc-gen-grpc-java=$REPO/$GRPC_JAVA --grpc-java_out=../app \
	--proto_path=$PROTO_SOURCE_DIR $PROTO_SOURCE_FILE
protoc -I=$PROTO_SOURCE_DIR --java_out=../app $PROTO_SOURCE_DIR/$PROTO_SOURCE_FILE

# Generate the gRPC Go interfaces.
go mod download
PATH=${GOPATH:-~/go}/bin:$PATH protoc $PROTO_SOURCE_DIR/$PROTO_SOURCE_FILE --go_out=plugins=grpc:.
