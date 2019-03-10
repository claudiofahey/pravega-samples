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
        logging.info('CreateScope response=%s' % response)
        response = stub.CreateStream(SimpleGrpcServer_pb2.CreateStreamRequest(scope=scope, stream=stream))
        logging.info('CreateStream response=%s' % response)

        events_to_write = [
            SimpleGrpcServer_pb2.WriteEventsRequest(scope=scope, stream=stream, event='write1'.encode(encoding='UTF-8')),
            SimpleGrpcServer_pb2.WriteEventsRequest(scope=scope, stream=stream, event='write2'.encode(encoding='UTF-8')),
            ]
        logging.info("events_to_write=%s", events_to_write);
        write_response = stub.WriteEvents(iter(events_to_write))
        logging.info("write_response=" + str(write_response))

        for r in stub.ReadEvents(SimpleGrpcServer_pb2.ReadEventsRequest(
                scope=scope, stream=stream, timeout_ms=3000)):
            event_string = r.event.decode(encoding='UTF-8')
            logging.info('ReadEvents: event_string=%s, response=%s' % (event_string, str(r)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
