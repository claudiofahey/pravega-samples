#!/usr/bin/env bash
set -ex

python -m grpc_tools.protoc \
-Isrc/main/proto/ \
--python_out=src/main/python \
--grpc_python_out=src/main/python \
src/main/proto/pravega/pravega.proto
