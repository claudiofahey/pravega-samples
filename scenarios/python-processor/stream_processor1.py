import logging
import grpc
import SimpleGrpcServer_pb2
import SimpleGrpcServer_pb2_grpc


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = SimpleGrpcServer_pb2_grpc.SimpleGrpcServerStub(channel)

        scope = 'examples3'
        input_stream = 'stream1'
        output_stream = 'stream2'

        response = stub.CreateScope(SimpleGrpcServer_pb2.CreateScopeRequest(scope=scope))
        logging.info('CreateScope response=%s' % response)
        response = stub.CreateStream(SimpleGrpcServer_pb2.CreateStreamRequest(scope=scope, stream=input_stream))
        logging.info('CreateStream response=%s' % response)
        response = stub.CreateStream(SimpleGrpcServer_pb2.CreateStreamRequest(scope=scope, stream=output_stream))
        logging.info('CreateStream response=%s' % response)

        read_events_request = SimpleGrpcServer_pb2.ReadEventsRequest(
                scope=scope, stream=input_stream, timeout_ms=2**63-1)
        logging.info('read_events_request=%s', read_events_request)
        for r in stub.ReadEvents(read_events_request):
            input_event_string = r.event.decode(encoding='UTF-8')
            logging.info('ReadEvents: input_event_string=%s, response=%s' % (input_event_string, str(r)))

            # Process event
            output_event_string = 'processed: ' + input_event_string

            # Write output event
            events_to_write = [
                SimpleGrpcServer_pb2.WriteEventsRequest(
                    scope=scope, stream=output_stream, event=output_event_string.encode(encoding='UTF-8')),
                ]
            logging.info("events_to_write=%s", events_to_write);
            write_response = stub.WriteEvents(iter(events_to_write))
            logging.info("write_response=" + str(write_response))
            # Output event is guaranteed to have been durably written.


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
