#!/bin/bash -xue
# Generates Java and Go interfaces from proto files. These interfaces are used
# for LynxKite-Sphynx communication.
# Compiles Sphynx.

cd $(dirname $0)
REPO=$(realpath .)
PROTO_SOURCE_DIR="proto"
PROTO_SOURCE_FILE="sphynx.proto"

# Get protobuf compiler.
if ! type "protoc" > /dev/null; then
  PROTOC_VERSION="3.10.0"
  PROTOC_ZIP="protoc-$PROTOC_VERSION-linux-x86_64.zip"
  wget -nc https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP
  mkdir -p protoc
  unzip -n $PROTOC_ZIP bin/protoc -d protoc
fi
PATH="${REPO}/protoc/bin:${PATH}"

# Generate the gRPC Java interfaces.
GRPC_JAVA_VERSION="1.24.0"
GRPC_JAVA=protoc-gen-grpc-java-$GRPC_JAVA_VERSION-linux-x86_64.exe
wget -nc https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/$GRPC_JAVA_VERSION/$GRPC_JAVA
chmod +x $REPO/$GRPC_JAVA
protoc --plugin=protoc-gen-grpc-java=$REPO/$GRPC_JAVA --grpc-java_out=../app \
	--proto_path=$PROTO_SOURCE_DIR $PROTO_SOURCE_FILE
protoc -I=$PROTO_SOURCE_DIR --java_out=../app $PROTO_SOURCE_DIR/$PROTO_SOURCE_FILE

GO_PKG=github.com/biggraph/biggraph/sphynx
export GOPATH=$REPO/go
export GOCACHE=$REPO/go/.cache

# Generate the gRPC Go interfaces.
go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
PATH=$GOPATH/bin:$PATH protoc $PROTO_SOURCE_DIR/$PROTO_SOURCE_FILE --go_out=plugins=grpc,import_path=$PROTO_SOURCE_DIR:.

# Compile Sphynx server.
cd $GOPATH/src/$GO_PKG
go fmt $GO_PKG/server
go get -v $GO_PKG/server
