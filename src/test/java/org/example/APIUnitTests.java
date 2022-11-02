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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ResponseWithValue;

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


}
