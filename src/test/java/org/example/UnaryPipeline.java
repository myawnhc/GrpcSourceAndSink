package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import org.example.grpc.GrpcConnector;

import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleResponse;

public class UnaryPipeline {
    public Pipeline createPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(GrpcConnector.<SimpleRequest>grpcSource("account.TestConnector", "singleToSingle"))
                .withoutTimestamps()
                .map(request -> {
                    SimpleResponse response = SimpleResponse.newBuilder()
                            .setResponse("Response to " + request.getRequest())
                            .build();
                    return response;
                })
                .writeTo(GrpcConnector.grpcSink("account.TestConnector", "singleToSingle"));
        return p;
    }

    public static void main(String[] args) {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        UnaryPipeline up = new UnaryPipeline();
        Pipeline p = up.createPipeline();
        hazelcast.getJet().newJob(p);
    }
}
