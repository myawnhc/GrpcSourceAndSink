package org.example;

import io.grpc.stub.StreamObserver;
import org.example.grpc.GrpcServer;
import org.hazelcast.grpcconnector.ExamplesGrpc;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;

public class SampleNonHZService extends ExamplesGrpc.ExamplesImplBase {

    public SampleNonHZService() {
        GrpcServer server = new GrpcServer(this, 50050);
    }

    @Override
    public void sayHello(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        SimpleResponse response = SimpleResponse.newBuilder()
                .setResponse("Hello, " + request.getRequest())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    // Starts GRPC server
    public static void main(String[] args) throws InterruptedException {
        SampleNonHZService service = new SampleNonHZService();
        // Run until terminated
        while(true)
            MINUTES.sleep(60);
    }
}
