Sphynx is an gRPC server. LynxKite can connect to it and ask it to do some work.
The idea is that Sphynx performs operations on graphs that fits into the memory,
so there's no need to do slow distributed computations.

To build it, run `./build.sh`.

To run it, run `SPHYNX_PORT=<port> go/bin/server`.

The port it's running on is defined in the environment variable `SPHYNX_PORT`.
(You can set it in the kiterc file.)

`grpc_cli` is a useful method to get information about a running gRPC server.
After [installation](https://github.com/grpc/grpc/blob/master/BUILDING.md),
you can send RPCs to the server from the command line:

grpc_cli call localhost:50051 CanCompute "operation: 'ProveRiemannHypothesis'" --protofiles=sphynx.proto
