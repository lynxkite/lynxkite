#!/bin/bash -xue
# Generates Java and Go interfaces from proto files. These interfaces are used
# for LynxKite-Sphynx communication.

cd $(dirname $0)
. sphynx_common.sh

# Get protobuf compiler.
if ! type "protoc" > /dev/null; then
  PROTOC_VERSION="3.10.0"
  PROTOC_ZIP="protoc-$PROTOC_VERSION-linux-x86_64.zip"
  wget -nv https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP
  mkdir -p protoc
  unzip -n $PROTOC_ZIP bin/protoc -d protoc
fi

# Generate the gRPC Java interfaces.
GRPC_JAVA_VERSION="1.24.0"
GRPC_JAVA=protoc-gen-grpc-java-$GRPC_JAVA_VERSION-linux-x86_64.exe
if ! -f $REPO/.build/$GRPC_JAVA; then
  wget -nv -P .build https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/$GRPC_JAVA_VERSION/$GRPC_JAVA
  chmod +x $REPO/.build/$GRPC_JAVA
fi
protoc --plugin=protoc-gen-grpc-java=$REPO/.build/$GRPC_JAVA --grpc-java_out=../app \
	--proto_path=$PROTO_SOURCE_DIR $PROTO_SOURCE_FILE
protoc -I=$PROTO_SOURCE_DIR --java_out=../app $PROTO_SOURCE_DIR/$PROTO_SOURCE_FILE

# Generate the gRPC Go interfaces.
go build -o .build/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go
go build -o .build/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc
PATH=.build:$PATH protoc $PROTO_SOURCE_DIR/$PROTO_SOURCE_FILE --go-grpc_out=proto --go_out=proto
# We don't need the whole directory tree. We are inside the module.
mv proto/github.com/lynxkite/lynxkite/sphynx/proto/* proto/
rm -rf proto/github.com
