import logging
import datetime
import time
import grpc
import SimpleGrpcServer_pb2
import SimpleGrpcServer_pb2_grpc


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = SimpleGrpcServer_pb2_grpc.SimpleGrpcServerStub(channel)

        scope = 'examples3'
        stream = 'stream1'

        response = stub.CreateScope(SimpleGrpcServer_pb2.CreateScopeRequest(scope=scope))
        logging.info('CreateScope response=%s' % response)
        response = stub.CreateStream(SimpleGrpcServer_pb2.CreateStreamRequest(scope=scope, stream=stream))
        logging.info('CreateStream response=%s' % response)

        while True:
            events_to_write = [
                SimpleGrpcServer_pb2.WriteEventsRequest(scope=scope, stream=stream, event=str(datetime.datetime.now()).encode(encoding='UTF-8')),
                ]
            logging.info("events_to_write=%s", events_to_write);
            write_response = stub.WriteEvents(iter(events_to_write))
            logging.info("write_response=" + str(write_response))
            time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
