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

import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.client.HazelcastClient;

import com.hazelcast.core.HazelcastInstance;
import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ResponseWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ChatMessage;

public class APIUnitTests {

    static HazelcastInstance hazelcast;
    static SampleClient client;

    @BeforeAll
    public static void init() {
        hazelcast = HazelcastClient.newHazelcastClient();
        client = new SampleClient();
    }

    @AfterAll
    public static void cleanUp() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testUnaryBlocking() {
        SimpleRequest request = SimpleRequest.newBuilder()
                .setRequest("Tony")
                .build();
        SimpleResponse response = client.sayHelloBlocking(request);
        System.out.println("Response: " + response.getResponse());
        Assertions.assertTrue(response.getResponse().contains("Hello,"));
    }

    @Test
    public void testUnaryAsync() {
        SimpleRequest request = SimpleRequest.newBuilder()
                .setRequest("John")
                .build();
        StreamObserver<SimpleResponse> observer = new StreamObserver<>() {
            private int responsesReceived = 0;
            @Override
            public void onNext(SimpleResponse simpleResponse) {
                System.out.println("Response: " + simpleResponse.getResponse());
                Assertions.assertTrue(simpleResponse.getResponse().contains("Hello,"));
                Assertions.assertEquals(1, ++responsesReceived);
            }

            @Override
            public void onError(Throwable throwable) {
                Assertions.fail(throwable);
            }

            @Override
            public void onCompleted() {

            }
        };
        client.sayHelloAsync(request, observer);
    }

    @Test
    public void testUnaryFuture() {
        SimpleRequest request = SimpleRequest.newBuilder()
                .setRequest("Bob")
                .build();
        ListenableFuture<SimpleResponse> future = client.sayHelloFuture(request);
        SimpleResponse response;
        try {
            response = future.get();
            System.out.println("Response: " + response.getResponse());
            Assertions.assertTrue(response.getResponse().contains("Hello,"));
        } catch (InterruptedException | ExecutionException e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testClientStreamingBlocking() throws InterruptedException {
        RequestWithValue m1 = RequestWithValue.newBuilder().setInputValue(42).build();
        RequestWithValue m2 = RequestWithValue.newBuilder().setInputValue(7).build();
        RequestWithValue m3 = RequestWithValue.newBuilder().setInputValue(21).build();
        RequestWithValue m4 = RequestWithValue.newBuilder().setInputValue(61).build();
        RequestWithValue m5 = RequestWithValue.newBuilder().setInputValue(6).build();

        // Because responses come back async, if we don't have this CountDownLatch we
        // will exit, trigger half-close of socket to server and premature termination
        final CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<ResponseWithValue> responseObserver = new StreamObserver<>() {
            private int responsesReceived = 0;

            @Override
            public void onNext(ResponseWithValue responseWithValue) {
                Assertions.assertEquals(137, responseWithValue.getOutputValue());
                Assertions.assertEquals(1, ++responsesReceived);
            }

            @Override
            public void onError(Throwable throwable) {
                Assertions.fail(throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        StreamObserver<RequestWithValue> requestObserver = client.addAsync(responseObserver);
        requestObserver.onNext(m1);
        requestObserver.onNext(m2);
        requestObserver.onNext(m3);
        requestObserver.onNext(m4);
        requestObserver.onNext(m5);
        requestObserver.onCompleted();
        latch.await(1, TimeUnit.MINUTES);
    }

    @Test
    public void testServerStreamingAsync() {
        SimpleRequest request = SimpleRequest.newBuilder().setRequest("myEvents").build();
        // Because responses come back async, if we don't have this CountDownLatch we
        // will exit, trigger half-close of socket to server and premature termination
        final CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<SimpleResponse> responseObserver = new StreamObserver<>() {
            private int responsesReceived = 0;

            @Override
            public void onNext(SimpleResponse simpleResponse) {
                responsesReceived++;
            }

            @Override
            public void onError(Throwable throwable) {
                Assertions.fail(throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
                Assertions.assertEquals(3, responsesReceived);
            }
        };
        client.subscribeAsync(request, responseObserver);
    }

    @Test
    public void testServerStreamingBlocking() {
        SimpleRequest request = SimpleRequest.newBuilder().setRequest("myEvents").build();
        // Returned iterator is a gRPC BlockingResponseStream
        Iterator<SimpleResponse> responseIterator = client.subscribeBlocking(request);
        int responseCount = 0;
        while (responseIterator.hasNext()) {
            SimpleResponse response = responseIterator.next();
            System.out.println("Streaming response: " + response);
            responseCount++;
        }
        Assertions.assertEquals(3, responseCount);
    }

    private String findAReceiver(String forSender, String[] fromCandidates) {
        // Randomly select a receiver for our messages - if we pick our own ID, redraw
        while (true) {
            int index = (int) (Math.random() * fromCandidates.length) ;
            //System.out.println("Given " + fromCandidates.length + " candidates we picked " + index);
            if (! fromCandidates[index].equals(forSender))
                return fromCandidates[index];
        }
    }

    @Test
    public void testBidirectionalStreamingAsync() throws InterruptedException {
        final int NUM_THREADS = 3;
        final int MESSAGES_PER_THREAD = 10;
        String[] identifiers = new String[NUM_THREADS];
        int totalMessagesSent = 0;
        final AtomicInteger totalMessagesReceived = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        // Need to establish all IDS up front so we can send to receivers
        // created later than the sender ...
        for (int i=0; i<NUM_THREADS; i++) {
            identifiers[i] = UUID.randomUUID().toString();
        }

        for (int i=0; i<NUM_THREADS; i++) {
            final int threadIndex = i;
            StreamObserver<ChatMessage> receiverStub = new StreamObserver<>() {
                @Override
                public void onNext(ChatMessage chatMessage) {
                    String intendedReceiver = chatMessage.getReceiverID();
                    String sender = chatMessage.getSenderID();
                    totalMessagesReceived.getAndIncrement();
                    System.out.println("Received message " + totalMessagesReceived + " on thread " + threadIndex);
                    // Make sure message indended for us
                    //Assertions.assertEquals(identifiers[threadIndex], intendedReceiver);
                }

                @Override
                public void onError(Throwable throwable) {
                    Assertions.fail(throwable);
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    //System.out.println("Stream marked complete");
                    latch.countDown();
                }
            };
            String sender = identifiers[i];
            StreamObserver<ChatMessage> senderStub = client.chat(receiverStub);
            for (int j=0; j<MESSAGES_PER_THREAD; j++) {
                String receiver = findAReceiver(sender, identifiers);
                if (sender == null || receiver == null) {
                    throw new IllegalArgumentException("Bad ID");
                }
                ChatMessage toSend = ChatMessage.newBuilder()
                        .setSenderID(sender)
                        .setReceiverID(receiver)
                        .setMessage("message " + j + " from " + sender + " to " + receiver)
                        .build();
                senderStub.onNext(toSend);
                totalMessagesSent++;
                //System.out.println("Send #" + totalMessagesSent + " " + toSend );
            }
            senderStub.onCompleted();
        }
        System.out.println("Finished sending " + totalMessagesSent + " chat messages, awaiting responses");
        latch.await(1, TimeUnit.MINUTES);
        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived.get());
    }


}
