#!/bin/bash -xue

REPO=$(realpath .)
PROTO_SOURCE_DIR="sphynx/proto"
PROTO_SOURCE_FILE="sphynx.proto"

GRPC_JAVA=protoc-gen-grpc-java-1.24.0-linux-x86_32.exe
wget -nc https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.24.0/$GRPC_JAVA
chmod +x $REPO/$GRPC_JAVA
protoc --plugin=protoc-gen-grpc-java=$REPO/$GRPC_JAVA --grpc-java_out=app \
	--proto_path=$PROTO_SOURCE_DIR $PROTO_SOURCE_FILE
protoc -I=$PROTO_SOURCE_DIR --java_out=app $PROTO_SOURCE_DIR/$PROTO_SOURCE_FILE

GO_PKG=github.com/biggraph/biggraph/sphynx
export GOPATH=$REPO/sphynx/go
export GOCACHE=$REPO/sphynx/go/.cache
SERVER=$GOPATH/bin/server
go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
PATH=$GOPATH/bin:$PATH protoc sphynx/proto/sphynx.proto --go_out=plugins=grpc,import_path=proto:.
cd $GOPATH/src/$GO_PKG
go fmt $GO_PKG/server
go get -v $GO_PKG/server

cd $REPO
