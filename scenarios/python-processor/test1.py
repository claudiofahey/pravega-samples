"""The Python implementation of the GRPC helloworld.Greeter client."""

from __future__ import print_function
import logging

import grpc

import SimpleGrpcServer_pb2
import SimpleGrpcServer_pb2_grpc


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = SimpleGrpcServer_pb2_grpc.SimpleGrpcServerStub(channel)

        response = stub.Test1(SimpleGrpcServer_pb2.Test1Request(name='Test1Me'))
        print("Test1 response: " + response.message)

        # response = stub.Test2(SimpleGrpcServer_pb2.Test1Request(name='Test2Me'))
        # print("Test2 response: " + response.message)

        # for streaming_response in stub.Test3(SimpleGrpcServer_pb2.Test1Request(name='Test3Me')):
        #     print('Test3 response: ' + str(streaming_response))
        #
        # for streaming_response in stub.Test4(SimpleGrpcServer_pb2.Test1Request(name='Test4Me')):
        #     print('Test4 response: ' + str(streaming_response))

        scope = 'examples'
        stream = 'stream2'

        events_to_write = [
            SimpleGrpcServer_pb2.WriteEventsRequest(scope=scope, stream=stream, event='write1'),
            SimpleGrpcServer_pb2.WriteEventsRequest(scope=scope, stream=stream, event='write2'),
            ]
        write_response = stub.WriteEvents(iter(events_to_write))
        print("write_response=" + str(write_response))

        for streaming_response in stub.ReadEvents(SimpleGrpcServer_pb2.ReadEventsRequest(scope=scope, stream=stream)):
            print('ReadEvents response: ' + str(streaming_response))


if __name__ == '__main__':
    logging.basicConfig()
    run()
