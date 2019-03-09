package io.pravega.example.simple_grpc_server;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown of a {@code SimpleGrpcServer} server.
 */
public class SimpleGrpcServer {
    private static final Logger logger = Logger.getLogger(SimpleGrpcServer.class.getName());

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new SimpleGrpcServerImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                SimpleGrpcServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final SimpleGrpcServer server = new SimpleGrpcServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class SimpleGrpcServerImpl extends SimpleGrpcServerGrpc.SimpleGrpcServerImplBase {

        @Override
        public void test1(Test1Request req, StreamObserver<Test1Reply> responseObserver) {
            Test1Reply reply = Test1Reply.newBuilder().setMessage("Hello " + req.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void test2(Test1Request req, StreamObserver<Test1Reply> responseObserver) {

            final int READER_TIMEOUT_MS = 2000;
            final String scope = "examples";
            final String streamName = "helloStream";
            final URI controllerURI = Parameters.getControllerURI();

            StreamManager streamManager = StreamManager.create(controllerURI);

            final boolean scopeIsNew = streamManager.createScope(scope);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

            final String readerGroup = UUID.randomUUID().toString().replace("-", "");
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamName))
                    .build();
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
                readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            }
            String message = "no message";

            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamReader<String> reader = clientFactory.createReader("reader",
                         readerGroup,
                         new JavaSerializer<String>(),
                         ReaderConfig.builder().build())) {
                System.out.format("Reading all the events from %s/%s%n", scope, streamName);
                EventRead<String> event = null;
                do {
                    try {
                        event = reader.readNextEvent(READER_TIMEOUT_MS);
                        if (event.getEvent() != null) {
                            System.out.format("Read event '%s'%n", event.getEvent());
                            message = event.getEvent();
                        }
                    } catch (ReinitializationRequiredException e) {
                        //There are certain circumstances where the reader needs to be reinitialized
                        e.printStackTrace();
                    }
                } while (event.getEvent() != null);
                System.out.format("No more events from %s/%s%n", scope, streamName);
            }

            Test1Reply reply = Test1Reply.newBuilder().setMessage("Hello " + req.getName() + ", message=" + message).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

//    @Override
//    public StreamObserver<Test1Request> test31(StreamObserver<Test1Reply> responseObserver) {
//      return new StreamObserver<Test1Request>() {
//        @Override
//        public void onNext(Test1Request req) {
//          Test1Reply reply = Test1Reply.newBuilder().setMessage("Hello " + req.getName()).build();
//          responseObserver.onNext(reply);
//          responseObserver.onCompleted();
//        }
//
//        @Override
//        public void onError(Throwable t) {
//          logger.log(Level.WARNING, "Encountered error", t);
//        }
//
//        @Override
//        public void onCompleted() {
//          responseObserver.onCompleted();
//        }
//      };
//    }

        @Override
        public void test3(Test1Request req, StreamObserver<Test1Reply> responseObserver) {
            Test1Reply reply = Test1Reply.newBuilder().setMessage("Hello " + req.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void test4(Test1Request req, StreamObserver<Test1Reply> responseObserver) {
            final int READER_TIMEOUT_MS = 2000;
            final String scope = "examples";
            final String streamName = "helloStream";
            final URI controllerURI = Parameters.getControllerURI();

            StreamManager streamManager = StreamManager.create(controllerURI);

            final boolean scopeIsNew = streamManager.createScope(scope);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

            final String readerGroup = UUID.randomUUID().toString().replace("-", "");
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamName))
                    .build();
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
                readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            }
            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamReader<String> reader = clientFactory.createReader("reader",
                         readerGroup,
                         new JavaSerializer<String>(),
                         ReaderConfig.builder().build())) {
                System.out.format("Reading all the events from %s/%s%n", scope, streamName);
                EventRead<String> event = null;
                do {
                    try {
                        event = reader.readNextEvent(READER_TIMEOUT_MS);
                        if (event.getEvent() != null) {
                            System.out.format("Read event '%s'%n", event.getEvent());
                            Test1Reply reply = Test1Reply.newBuilder().setMessage("Hello " + req.getName() + ", message=" + event.getEvent()).build();
                            responseObserver.onNext(reply);

                        }
                    } catch (ReinitializationRequiredException e) {
                        //There are certain circumstances where the reader needs to be reinitialized
                        e.printStackTrace();
                    }
                } while (event.getEvent() != null);
                System.out.format("No more events from %s/%s%n", scope, streamName);
            }

            responseObserver.onCompleted();
        }

        @Override
        public void readEvents(ReadEventsRequest req, StreamObserver<ReadEventsResponse> responseObserver) {
            final int READER_TIMEOUT_MS = 2000;
            final URI controllerURI = Parameters.getControllerURI();
            final String scope = req.getScope();
            final String streamName = req.getStream();

            StreamManager streamManager = StreamManager.create(controllerURI);
            streamManager.createScope(scope);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            streamManager.createStream(scope, streamName, streamConfig);

            final String readerGroup = UUID.randomUUID().toString().replace("-", "");
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamName))
                    .build();
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
                readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            }

            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamReader<ByteBuffer> reader = clientFactory.createReader("reader",
                         readerGroup,
                         new ByteBufferSerializer(),
                         ReaderConfig.builder().build())) {
                System.out.format("Reading all the events from %s/%s%n", scope, streamName);
                for (; ; ) {
                    try {
                        EventRead<ByteBuffer> event = reader.readNextEvent(READER_TIMEOUT_MS);
                        if (event.isCheckpoint()) {
                            ReadEventsResponse response = ReadEventsResponse.newBuilder()
                                    .setCheckpointName(event.getCheckpointName())
                                    .build();
                            logger.info("readEvents: response=" + response.toString());
                            responseObserver.onNext(response);
                        } else if (event.getEvent() != null) {
                            ReadEventsResponse response = ReadEventsResponse.newBuilder()
                                    .setEvent(ByteString.copyFrom(event.getEvent()))
                                    .setPosition(event.getPosition().toString())
                                    .setEventPointer(event.getEventPointer().toString())
                                    .build();
                            logger.info("readEvents: response=" + response.toString());
                            responseObserver.onNext(response);
                        } else {
                            break;
                        }
                    } catch (ReinitializationRequiredException e) {
                        // There are certain circumstances where the reader needs to be reinitialized
                        e.printStackTrace();
                    }
                }
                System.out.format("No more events from %s/%s%n", scope, streamName);
            }

            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<WriteEventsRequest> writeEvents(StreamObserver<WriteEventsResponse> responseObserver) {
            return new StreamObserver<WriteEventsRequest>() {
                @Override
                public void onNext(WriteEventsRequest req) {
                    logger.info("writeEvents: req=" + req.toString());
                    final URI controllerURI = Parameters.getControllerURI();
                    final String scope = req.getScope();
                    final String streamName = req.getStream();

                    StreamManager streamManager = StreamManager.create(controllerURI);
                    streamManager.createScope(scope);
                    StreamConfiguration streamConfig = StreamConfiguration.builder()
                            .scalingPolicy(ScalingPolicy.fixed(1))
                            .build();
                    streamManager.createStream(scope, streamName, streamConfig);

                    try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                         EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(
                                 streamName,
                                 new ByteBufferSerializer(),
                                 EventWriterConfig.builder().build())) {
                        writer.writeEvent(req.getRoutingKey(), req.getEvent().asReadOnlyByteBuffer());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "Encountered error in writeEvents", t);
                }

                @Override
                public void onCompleted() {
                    WriteEventsResponse response = WriteEventsResponse.newBuilder()
                            .build();
                    logger.info("writeEvents: response=" + response.toString());
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
