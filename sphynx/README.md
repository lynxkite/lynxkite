Sphynx is a gRPC server. LynxKite can connect to it and ask it to do some work.
The idea is that Sphynx performs operations on graphs that fits into the memory,
so there's no need to do slow distributed computations.

To build it, run `./build.sh`.

If you start LynxKite with `run.sh` or `stage/bin/biggraph`, it will start Sphynx as well.
The port it's running on is defined in the environment variable `SPHYNX_PORT`, you can set
it in the kiterc file. If you want to run it alone, run `SPHYNX_PORT=<port> go/bin/server`.

`grpc_cli` is a useful tool to get information about a running gRPC server.
After [installation](https://github.com/grpc/grpc/blob/master/BUILDING.md),
you can send RPCs to the server from the command line:

`grpc_cli call localhost:50051 CanCompute "operation: 'ProveRiemannHypothesis'" --protofiles=sphynx.proto`

If you want to use an encrypted channel between LynxKite and Sphynx,
you may generate a self-signed certificate by running `./server/generate_cert.sh`.
Then you can start a new server that uses the generated private key and certificate by
`SPHYNX_PORT=<port> go/bin/server -keydir=<directory of cert.pem and private-key.pem files>`.
LynxKite expects a certification file at the path stored in the environment variable `$SPHYNX_CERT_DIR`.
