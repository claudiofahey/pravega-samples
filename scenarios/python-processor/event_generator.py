#!/usr/bin/env python

import logging
import datetime
import time
import grpc
import pravega


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(channel)

        scope = 'examples4'
        stream = 'stream1'

        response = pravega_client.CreateScope(pravega.pb.CreateScopeRequest(scope=scope))
        logging.info('CreateScope response=%s' % response)
        response = pravega_client.CreateStream(pravega.pb.CreateStreamRequest(scope=scope, stream=stream))
        logging.info('CreateStream response=%s' % response)

        while True:
            events_to_write = [
                pravega.pb.WriteEventsRequest(scope=scope, stream=stream, event=str(datetime.datetime.now()).encode(encoding='UTF-8')),
                ]
            logging.info("events_to_write=%s", events_to_write);
            write_response = pravega_client.WriteEvents(iter(events_to_write))
            logging.info("write_response=" + str(write_response))
            time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()