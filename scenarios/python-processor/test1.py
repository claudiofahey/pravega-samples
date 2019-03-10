"""The Python implementation of the GRPC helloworld.Greeter client."""

from __future__ import print_function
import logging

import grpc

import SimpleGrpcServer_pb2
import SimpleGrpcServer_pb2_grpc


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = SimpleGrpcServer_pb2_grpc.SimpleGrpcServerStub(channel)

        scope = 'examples2'
        stream = 'stream1'

        response = stub.CreateScope(SimpleGrpcServer_pb2.CreateScopeRequest(scope=scope))
        print('CreateScope response=%s' % response)
        response = stub.CreateStream(SimpleGrpcServer_pb2.CreateStreamRequest(scope=scope, stream=stream))
        print('CreateStream response=%s' % response)

        events_to_write = [
            SimpleGrpcServer_pb2.WriteEventsRequest(scope=scope, stream=stream, event='write1'.encode(encoding='UTF-8')),
            SimpleGrpcServer_pb2.WriteEventsRequest(scope=scope, stream=stream, event='write2'.encode(encoding='UTF-8')),
            ]
        write_response = stub.WriteEvents(iter(events_to_write))
        print("write_response=" + str(write_response))

        for r in stub.ReadEvents(SimpleGrpcServer_pb2.ReadEventsRequest(scope=scope, stream=stream)):
            event_string = r.event.decode(encoding='UTF-8')
            print('ReadEvents: event=%s, event_string=%s, response=%s' % (r.event, event_string, str(r)))


if __name__ == '__main__':
    logging.basicConfig()
    run()
