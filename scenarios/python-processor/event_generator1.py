#!/usr/bin/env python

import logging
import datetime
import time
import grpc
import pravega
# from pravega.grpc import SimpleGrpcServerStub
# from pravega.pb import CreateScopeRequest, CreateStreamRequest, WriteEventsRequest, SimpleGrpcServerStub


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = pravega.grpc.PravegaServerStub(channel)

        scope = 'examples4'
        stream = 'stream1'

        response = stub.CreateScope(pravega.pb.CreateScopeRequest(scope=scope))
        logging.info('CreateScope response=%s' % response)
        response = stub.CreateStream(pravega.pb.CreateStreamRequest(scope=scope, stream=stream))
        logging.info('CreateStream response=%s' % response)

        while True:
            events_to_write = [
                pravega.pb.WriteEventsRequest(scope=scope, stream=stream, event=str(datetime.datetime.now()).encode(encoding='UTF-8')),
                ]
            logging.info("events_to_write=%s", events_to_write);
            write_response = stub.WriteEvents(iter(events_to_write))
            logging.info("write_response=" + str(write_response))
            time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
