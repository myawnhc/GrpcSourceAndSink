package org.example;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.hazelcast.grpcconnector.TestConnectorGrpc;

import java.util.logging.Logger;

import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleResponse;

public class SampleClient {
    private static final Logger logger = Logger.getLogger(SampleClient.class.getName());

    private final TestConnectorGrpc.TestConnectorStub stub;
    private final TestConnectorGrpc.TestConnectorBlockingStub blockingStub;
    private final TestConnectorGrpc.TestConnectorFutureStub futureStub;
    private final ManagedChannel channel;

    public SampleClient() {
        String target = "localhost:50050";
        channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        // Not sure different stubs will affect the behavior of pipeline so we might
        // actually only need one of them ... but good to document all the ways that
        // this gRPC APIs could be used
        stub = TestConnectorGrpc.newStub(channel);
        blockingStub = TestConnectorGrpc.newBlockingStub(channel);
        futureStub = TestConnectorGrpc.newFutureStub(channel);
    }

    // Methods to support unit tests
    public void singleToSingle(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        stub.singleToSingle(request, responseObserver);
    }
    public SimpleResponse singleToSingleBlocking(SimpleRequest request) {
        return blockingStub.singleToSingle(request);
    }
    public ListenableFuture<SimpleResponse> singleToSingleFuture(SimpleRequest request) {
        return futureStub.singleToSingle(request);
    }

//    public static void main(String[] args) {
//        SampleClient client = new SampleClient();
//
//        // Test Unary method - should be moved to a Junit class that uses
//        // per-API methods exposed by the SampleClient
//        // This will expand to TestUnary, TestBlockingUnary, TestFutureUnary
//        // Then same 3 varitions of ClientStreaming, ServerStreaming, BidirectionalStreaming
//        SimpleRequest request = SimpleRequest.newBuilder()
//                .setRequest("unary method request")
//                .build();
//        System.out.println("Sending request: " + request.getRequest());
//        SimpleResponse response = client.singleToSingleBlocking(request);
//        System.out.println("Got response: " + response.getResponse());
//
//    }

}
