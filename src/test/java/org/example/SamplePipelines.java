package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import org.example.grpc.GrpcConnector;
import org.example.grpc.MessageWithUUID;
import org.example.grpc.StreamingMessage;

import java.util.UUID;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ResponseWithValue;


public class SamplePipelines {
    private Pipeline createUnaryPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(GrpcConnector.<MessageWithUUID<SimpleRequest>>grpcSource("hz.grpc.Examples", "sayHello"))
                .withoutTimestamps()
                .map(request -> {
                    UUID identifier = request.getIdentifier();
                    SimpleResponse response = SimpleResponse.newBuilder()
                            .setResponse("Hello, " + request.getMessage().getRequest())
                            .build();
                    return new MessageWithUUID(identifier,response);
                })
                .writeTo(GrpcConnector.grpcSink("hz.grpc.Examples", "sayHello"));
        return p;
    }

    private Pipeline createClientStreamingPipeline() {

        Pipeline p = Pipeline.create();
        p.setPreserveOrder(true); // seeing 'onComplete' before all streaming inputs are seen!

        p.readFrom(GrpcConnector.<StreamingMessage<RequestWithValue>>grpcSource("hz.grpc.Examples", "add"))
                .withoutTimestamps()
                .map(wrappedMessage -> Tuple3.tuple3(wrappedMessage.getMessage().getIdentifier(),
                        wrappedMessage.getMessage().getMessage(), wrappedMessage.isComplete()))
                .groupingKey(tuple3 -> tuple3.f0())
                .rollingAggregate(RequestWithValueAggregator.build())
                .filter(keyedTuple3 -> keyedTuple3.getValue().f2())
                .map(keyedTuple3 -> {
                    UUID identifier = keyedTuple3.getKey(); // or getValue().f0(), they both have the UUID
                    // Accumulator stuff results into a RequestWithValue - this isn't one of our input items
                    RequestWithValue request = keyedTuple3.getValue().f1();
                    int accumulatedValue = request.getInputValue();
                    ResponseWithValue response = ResponseWithValue.newBuilder()
                            .setOutputValue(accumulatedValue)
                            .build();
                    MessageWithUUID<ResponseWithValue> wrappedResult = new MessageWithUUID<>(identifier, response);
                    return wrappedResult;
                })
                .writeTo(GrpcConnector.grpcSink("hz.grpc.Examples", "add"));

        return p;
    }

    public static void main(String[] args) {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        SamplePipelines up = new SamplePipelines();

        JobConfig jc1 = new JobConfig().setName("sayHello");
        Pipeline unary = up.createUnaryPipeline();
        hazelcast.getJet().newJob(unary, jc1);

        JobConfig jc2 = new JobConfig().setName("add");
        Pipeline clientStreaming = up.createClientStreamingPipeline();
        hazelcast.getJet().newJob(clientStreaming, jc2);
    }
}
