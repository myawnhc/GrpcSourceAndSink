/*
 *
 *  * Copyright (c) Hazelcast, Inc. 2022-2023.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *
 *
 */

package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import org.example.grpc.GrpcConnector;
import org.example.grpc.MessageWithUUID;
import org.example.grpc.StreamingMessage;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.*;

public class SamplePipelines {
    private Pipeline createUnaryPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(GrpcConnector.<SimpleRequest>grpcUnarySource("hz.grpc.Examples", "sayHello"))
                .withoutTimestamps()
                .map(request -> {
                    UUID identifier = request.getIdentifier();
                    String name = request.getMessage().getRequest();
                    SimpleResponse response = SimpleResponse.newBuilder()
                            .setResponse("Hello, " + name)
                            .build();
                    return new MessageWithUUID(identifier,response);
                })
                .writeTo(GrpcConnector.grpcUnarySink("hz.grpc.Examples", "sayHello"));
        return p;
    }

    private Pipeline createClientStreamingPipeline() {

        Pipeline p = Pipeline.create();
        p.setPreserveOrder(true); // seeing 'onComplete' before all streaming inputs are seen!

        p.readFrom(GrpcConnector.<RequestWithValue>grpcStreamingSource("hz.grpc.Examples", "add"))
                .withoutTimestamps()
                .map(streamingMessage -> Tuple3.tuple3(streamingMessage.getIdentifier(),
                            streamingMessage.getMessage(),
                            streamingMessage.isComplete()))
                .groupingKey(Tuple3::f0)
                .rollingAggregate(RequestWithValueAggregator.build())
                .filter(keyedTuple3 -> Boolean.TRUE.equals(keyedTuple3.getValue().f2()))
                .map(keyedTuple3 -> {
                    UUID identifier = keyedTuple3.getKey(); // or getValue().f0(), they both have the UUID
                    RequestWithValue request = keyedTuple3.getValue().f1();
                    int accumulatedValue = request.getInputValue();
                    ResponseWithValue response = ResponseWithValue.newBuilder()
                            .setOutputValue(accumulatedValue)
                            .build();
                    MessageWithUUID<ResponseWithValue> wrappedResult = new MessageWithUUID<>(identifier, response);
                    return wrappedResult;
                })
                .writeTo(GrpcConnector.grpcUnarySink("hz.grpc.Examples", "add"));

        return p;
    }

    private Pipeline createServerStreamingPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(GrpcConnector.<SimpleRequest>grpcUnarySource("hz.grpc.Examples", "subscribe"))
                .withoutTimestamps()
                .flatMap(request -> {
                    ArrayList<StreamingMessage<SimpleResponse>> events = new ArrayList<>();
                    //System.out.println("in server streaming pipeline, expecting MWUID<SR>,  request is " + request);
                    UUID identifier = request.getIdentifier();
                    String name = request.getMessage().getRequest();
                    for (int i = 0; i < 3; i++) {
                        SimpleResponse response = SimpleResponse.newBuilder()
                                .setResponse(name + " event " + i)
                                .build();
                        events.add(new StreamingMessage<>(identifier, response, false));
                        //return new MessageWithUUID(identifier,response);
                    }
                    // Add completion message
                    events.add(new StreamingMessage<>(identifier, SimpleResponse.getDefaultInstance(), true));
                    return Traversers.traverseIterable(events);
                })
                .writeTo(GrpcConnector.grpcStreamingSink("hz.grpc.Examples", "subscribe"));
        return p;
    }

    private Pipeline createBidirectionalStreamingPipeline() {
        Pipeline p = Pipeline.create();
        p.setPreserveOrder(true); // Appears completions may move ahead of legit messages, experimenting with this setting
        AtomicInteger receivedCount = new AtomicInteger();
        p.readFrom(GrpcConnector.<ChatMessage>grpcStreamingSource("hz.grpc.Examples", "chat"))
                .withoutTimestamps()
                .map(streamingMessage -> {
                    System.out.println("Pipeline received " + receivedCount.incrementAndGet() + ":" + streamingMessage.getMessage().getMessage() + ":" + streamingMessage.isComplete());
                    String receiverID = streamingMessage.getMessage().getReceiverID();
                    boolean isComplete = streamingMessage.isComplete();
                    // Completed messages don't have sender and receiver set is the message body; additionally,
                    // a completed message means the sender is finished and its queue can be cleaned up -- the
                    // receivers queue may still be needed.  So when we see completion, we substitute the
                    // senders identity for the receiver since (a) there is no message to deliver and (b) our
                    // cleanup needs to be triggered for the sender
                    if (isComplete) {
                        System.out.println("Pipeline adjusting receiver on completion message " + receiverID + "->" + streamingMessage.getIdentifier());
                        System.out.flush();
                        receiverID = streamingMessage.getIdentifier().toString();
                    }
                    return new StreamingMessage<>(UUID.fromString(receiverID),
                            streamingMessage.getMessage(),
                            streamingMessage.isComplete());
                })
                .writeTo(GrpcConnector.grpcStreamingSink("hz.grpc.Examples", "chat"));
        return p;
    }

    public static void main(String[] args) {
//        Config config = new Config();
//        config.getJetConfig().setEnabled(true);
//        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

        HazelcastInstance hazelcast = HazelcastClient.newHazelcastClient();
        SamplePipelines up = new SamplePipelines();

        JobConfig jc1 = new JobConfig().setName("sayHello");
        Pipeline unary = up.createUnaryPipeline();
        hazelcast.getJet().newJob(unary, jc1);

        JobConfig jc2 = new JobConfig().setName("add");
        Pipeline clientStreaming = up.createClientStreamingPipeline();
        hazelcast.getJet().newJob(clientStreaming, jc2);

        JobConfig jc3 = new JobConfig().setName("subscribe");
        Pipeline serverStreaming = up.createServerStreamingPipeline();
        hazelcast.getJet().newJob(serverStreaming, jc3);

        JobConfig jc4 = new JobConfig().setName("chat");
        Pipeline bidiStreaming = up.createBidirectionalStreamingPipeline();
        hazelcast.getJet().newJob(bidiStreaming, jc4);
    }
}
