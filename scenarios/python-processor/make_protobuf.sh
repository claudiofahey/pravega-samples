#!/usr/bin/env bash
set -ex
source venv/bin/activate
python -m grpc_tools.protoc -I../simple-grpc-server/src/main/proto/ \
--python_out=. --grpc_python_out=. \
../simple-grpc-server/src/main/proto/SimpleGrpcServer.proto
