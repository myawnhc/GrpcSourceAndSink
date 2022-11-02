package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.grpc.stub.StreamObserver;
import org.example.grpc.APIBufferPair;
import org.example.grpc.GrpcServer;
import org.example.grpc.MessageWithUUID;
import org.example.grpc.StreamingMessage;
import org.hazelcast.grpcconnector.ExamplesGrpc;

import java.util.UUID;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ResponseWithValue;


public class SampleService extends ExamplesGrpc.ExamplesImplBase {

    private final String serviceName;
    private final IMap<String, APIBufferPair> bufferPairsForAPI;

    public SampleService(HazelcastInstance hazelcast) {
        GrpcServer server = new GrpcServer(this, 50050);
        serviceName = bindService().getServiceDescriptor().getName();
        System.out.println("SampleService starting to serve requests for " + serviceName);
        bufferPairsForAPI = hazelcast.getMap(serviceName+"_APIS");

        APIBufferPair<MessageWithUUID<SimpleRequest>,SimpleResponse> unaryHandler = new APIBufferPair(hazelcast,"sayHello");
        bufferPairsForAPI.put("sayHello", unaryHandler);

        APIBufferPair<StreamingMessage<RequestWithValue>,StreamingMessage<ResponseWithValue>>
                clientStreamingHandler = new APIBufferPair(hazelcast, "add");
        bufferPairsForAPI.put("add", clientStreamingHandler);

        // TODO: APIHandlers for other RPCs
    }

    @Override
    public void sayHello(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        APIBufferPair<MessageWithUUID<SimpleRequest>,SimpleResponse> unaryHandler = bufferPairsForAPI.get("sayHello");
        UUID identifier = UUID.randomUUID();
        unaryHandler.putRequest(new MessageWithUUID(identifier, request));
        SimpleResponse response = unaryHandler.getResponse(identifier);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<RequestWithValue> add(StreamObserver<ResponseWithValue> responseObserver) {
        APIBufferPair<StreamingMessage<RequestWithValue>,ResponseWithValue> bufferPair = bufferPairsForAPI.get("add");
        StreamObserver<RequestWithValue> requestObserver = new StreamObserver<>() {
            UUID clientIdentifier = UUID.randomUUID();
            @Override
            public void onNext(RequestWithValue request) {
                MessageWithUUID<RequestWithValue> messageWithUUID = new MessageWithUUID<>(clientIdentifier, request);
                StreamingMessage<RequestWithValue> wrappedMessage = new StreamingMessage<>(messageWithUUID);
                bufferPair.putRequest(wrappedMessage);
            }

            @Override
            public void onError(Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {
                RequestWithValue emptyMessage = RequestWithValue.newBuilder().getDefaultInstanceForType();
                MessageWithUUID<RequestWithValue> messageWithUUID = new MessageWithUUID<>(clientIdentifier, emptyMessage);
                StreamingMessage<RequestWithValue> completion = new StreamingMessage<>(messageWithUUID, true);
                bufferPair.putRequest(completion);
                System.out.println("Server side: Input marked complete, fetching response");

                // Once completion signaled, we expect response to be available
                System.out.println("Getting accumulated response - may block ");
                ResponseWithValue response = bufferPair.getResponse(clientIdentifier);
                System.out.println("Server side: Response prior to sending to client" + response);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
        return requestObserver;
    }

    // Starts GRPC server
    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        SampleService service = new SampleService(hazelcast);
        // Run until terminated
        while(true)
            MINUTES.sleep(60);
    }
}
