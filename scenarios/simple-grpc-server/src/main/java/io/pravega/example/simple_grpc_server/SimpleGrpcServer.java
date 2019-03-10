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
import java.util.concurrent.CompletableFuture;
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
        public void createScope(CreateScopeRequest req, StreamObserver<CreateScopeResponse> responseObserver) {
            StreamManager streamManager = StreamManager.create(Parameters.getControllerURI());
            boolean created = streamManager.createScope(req.getScope());
            responseObserver.onNext(CreateScopeResponse.newBuilder().setCreated(created).build());
            responseObserver.onCompleted();
        }

        @Override
        public void createStream(CreateStreamRequest req, StreamObserver<CreateStreamResponse> responseObserver) {
            StreamManager streamManager = StreamManager.create(Parameters.getControllerURI());
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            boolean created = streamManager.createStream(req.getScope(), req.getStream(), streamConfig);
            responseObserver.onNext(CreateStreamResponse.newBuilder().setCreated(created).build());
            responseObserver.onCompleted();
        }

        @Override
        public void readEvents(ReadEventsRequest req, StreamObserver<ReadEventsResponse> responseObserver) {
            final int READER_TIMEOUT_MS = 2000;
            final URI controllerURI = Parameters.getControllerURI();
            final String scope = req.getScope();
            final String streamName = req.getStream();

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
                        // TODO: Handle this error
                    }
                }
                System.out.format("No more events from %s/%s%n", scope, streamName);
            }

            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<WriteEventsRequest> writeEvents(StreamObserver<WriteEventsResponse> responseObserver) {
            return new StreamObserver<WriteEventsRequest>() {
                ClientFactory clientFactory;
                EventStreamWriter<ByteBuffer> writer;

                @Override
                public void onNext(WriteEventsRequest req) {
                    logger.info("writeEvents: req=" + req.toString());
                    if (writer == null) {
                        final URI controllerURI = Parameters.getControllerURI();
                        final String scope = req.getScope();
                        final String streamName = req.getStream();
                        clientFactory = ClientFactory.withScope(scope, controllerURI);
                        writer = clientFactory.createEventWriter(
                                streamName,
                                new ByteBufferSerializer(),
                                EventWriterConfig.builder().build());
                    }
                    final CompletableFuture writeFuture = writer.writeEvent(req.getRoutingKey(), req.getEvent().asReadOnlyByteBuffer());
                    // Wait for acknowledgement that the event was durably persisted.
                    // TODO: Wait should be an option that can be set by each request.
//                    writeFuture.get();

                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "Encountered error in writeEvents", t);
                }

                @Override
                public void onCompleted() {
                    writer.close();
                    clientFactory.close();
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
