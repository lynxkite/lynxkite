Sphynx is a gRPC server. LynxKite can connect to it and ask it to do some work.
The idea is that Sphynx performs operations on graphs that fit into the memory,
so there's no need to do slow distributed computations.

To build it, run `./build.sh`.

If you start LynxKite with `run.sh` or `stage/bin/biggraph`, it will start Sphynx as well.
The port it's running on is defined in the environment variable `SPHYNX_PORT`, you can set
it in the kiterc file. LynxKite expects a certification file at the path stored in the
environment variable `$SPHYNX_CERT_DIR`. If there is none, then the certificate is generated
on start-up.

If you want to run Sphynx alone, run
`SPHYNX_PORT=<port> go/bin/lynxkite-sphynx -keydir=<directory of cert.pem and private-key.pem files>`.
If no keydir is provided, then this starts a server without encryption. LynxKite
can communicate with Sphynx only through encrypted channels, so this setup is only useful
if you want to send requests manually. (E.g. from the command line for debugging purposes.)

`grpc_cli` is a useful tool to get information about a running gRPC server.
After [installation](https://github.com/grpc/grpc/blob/master/BUILDING.md),
you can send RPCs to the server from the command line:

`grpc_cli call localhost:50051 CanCompute "operation: 'ProveRiemannHypothesis'" --protofiles=sphynx.proto`
