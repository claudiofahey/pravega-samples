#!/usr/bin/env python

import logging
import grpc
import pravega


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(channel)

        scope = 'examples4'
        input_stream = 'stream1'
        output_stream = 'stream2'

        response = pravega_client.CreateScope(pravega.pb.CreateScopeRequest(scope=scope))
        logging.info('CreateScope response=%s' % response)
        response = pravega_client.CreateStream(pravega.pb.CreateStreamRequest(scope=scope, stream=input_stream))
        logging.info('CreateStream response=%s' % response)
        response = pravega_client.CreateStream(pravega.pb.CreateStreamRequest(scope=scope, stream=output_stream))
        logging.info('CreateStream response=%s' % response)

        read_events_request = pravega.pb.ReadEventsRequest(
                scope=scope, stream=input_stream, timeout_ms=2**63-1)
        logging.info('read_events_request=%s', read_events_request)
        for r in pravega_client.ReadEvents(read_events_request):
            logging.info('ReadEvents: response=%s' % str(r))

            # Process event
            if len(r.event) > 0:
                input_event_string = r.event.decode(encoding='UTF-8')
                output_event_string = 'processed: ' + input_event_string

                # Write output event
                events_to_write = [
                    pravega.pb.WriteEventsRequest(
                        scope=scope, stream=output_stream, event=output_event_string.encode(encoding='UTF-8')),
                    ]
                logging.info("events_to_write=%s", events_to_write)
                write_response = pravega_client.WriteEvents(iter(events_to_write))
                logging.info("write_response=" + str(write_response))
                # Output event is guaranteed to have been durably written.


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
