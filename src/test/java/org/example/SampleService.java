package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.grpc.stub.StreamObserver;
import org.example.grpc.APIBufferPair;
import org.example.grpc.GrpcServer;
import org.hazelcast.grpcconnector.TestConnectorGrpc;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleResponse;

public class SampleService extends TestConnectorGrpc.TestConnectorImplBase {

    private final String serviceName;
    private final IMap<String, APIBufferPair> handlerMap;

    public SampleService(HazelcastInstance hazelcast) {
        GrpcServer server = new GrpcServer(this, 50050);
        serviceName = bindService().getServiceDescriptor().getName();
        handlerMap = hazelcast.getMap(serviceName+"_APIS");
        APIBufferPair<SimpleRequest,SimpleResponse> unaryHandler = new APIBufferPair(hazelcast,"singleToSingle");
        handlerMap.put("singleToSingle", unaryHandler);
        // TODO: APIHandlers for other RPCs
    }

    @Override
    public void singleToSingle(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        APIBufferPair<SimpleRequest,SimpleResponse> unaryHandler = handlerMap.get("singleToSingle");
        unaryHandler.putRequest(request);
        SimpleResponse response = unaryHandler.getResponse();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
