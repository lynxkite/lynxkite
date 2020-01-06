REPO=$(realpath .)
PROTO_SOURCE_DIR="proto"
PROTO_SOURCE_FILE="sphynx.proto"
PATH="${REPO}/protoc/bin:${PATH}"
GO_PKG=github.com/biggraph/biggraph/sphynx
export GOPATH=$REPO/go
export GOCACHE=$REPO/go/.cache
