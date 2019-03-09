package io.pravega.example.simple_grpc_server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
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
  }
}
