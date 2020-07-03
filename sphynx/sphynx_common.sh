REPO=$(realpath .)
PROTO_SOURCE_DIR="proto"
PROTO_SOURCE_FILE="sphynx.proto"
PATH="${REPO}/protoc/bin:${PATH}"
GO_PKG=github.com/lynxkite/lynxkite/sphynx
mkdir -p .build
