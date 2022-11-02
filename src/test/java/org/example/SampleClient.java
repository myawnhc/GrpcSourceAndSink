package org.example;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.hazelcast.grpcconnector.ExamplesGrpc;

import java.util.logging.Logger;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ResponseWithValue;
import static org.hazelcast.grpcconnector.ExamplesGrpc.ExamplesBlockingStub;
import static org.hazelcast.grpcconnector.ExamplesGrpc.ExamplesFutureStub;
import static org.hazelcast.grpcconnector.ExamplesGrpc.ExamplesStub;

public class SampleClient {
    private static final Logger logger = Logger.getLogger(SampleClient.class.getName());

    private final ExamplesStub asyncStub;
    private final ExamplesBlockingStub blockingStub;
    private final ExamplesFutureStub futureStub;
    private final ManagedChannel channel;

    public SampleClient() {
        String target = "localhost:50050";
        channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        // Not all interaction styles are supported against all stubs; see the unit tests for legal combinations
        asyncStub    = ExamplesGrpc.newStub(channel);
        blockingStub = ExamplesGrpc.newBlockingStub(channel);
        futureStub   = ExamplesGrpc.newFutureStub(channel);
    }

    // Client-side implementation of the API
    public void sayHelloAsync(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        asyncStub.sayHello(request, responseObserver);
    }
    public SimpleResponse sayHelloBlocking(SimpleRequest request) {
        return blockingStub.sayHello(request);
    }
    public ListenableFuture<SimpleResponse> sayHelloFuture(SimpleRequest request) {
        return futureStub.sayHello(request);
    }

    public StreamObserver<RequestWithValue> addAsync(StreamObserver<ResponseWithValue> responseObserver) {
        // This is not available via the blocking or future stub
        StreamObserver<RequestWithValue> requestObserver = asyncStub.add(responseObserver);
        return requestObserver;
    }
}
