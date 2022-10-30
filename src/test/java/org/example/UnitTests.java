package org.example;

import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.client.HazelcastClient;

import com.hazelcast.core.HazelcastInstance;
import io.grpc.stub.StreamObserver;
import org.hazelcast.grpcconnector.TestConnectorOuterClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.TestConnectorOuterClass.SimpleResponse;

public class UnitTests {

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

    // TODO: Unary tests now done; add for ClientStreaming, ServerStreaming, BidirectionalStreaming

    @Test
    public void testUnaryBlocking() {
        // This will expand to TestUnary, TestUnaryBlocking, TestUnaryFuture
        // Then same 3 varitions of ClientStreaming, ServerStreaming, BidirectionalStreaming
        SimpleRequest request = TestConnectorOuterClass.SimpleRequest.newBuilder()
                .setRequest("unary method request")
                .build();
        SimpleResponse response = client.singleToSingleBlocking(request);
        Assertions.assertTrue(response.getResponse().contains("Response to"));
    }

    @Test
    public void testUnary() {
        TestConnectorOuterClass.SimpleRequest request = TestConnectorOuterClass.SimpleRequest.newBuilder()
                .setRequest("unary method request")
                .build();
        //System.out.println("Sending request: " + request.getRequest());
        StreamObserver<SimpleResponse> observer = new StreamObserver<>() {
            private int responsesReceived = 0;
            @Override
            public void onNext(SimpleResponse simpleResponse) {
                Assertions.assertTrue(simpleResponse.getResponse().contains("Response to"));
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
        client.singleToSingle(request, observer);
    }

    @Test
    public void testUnaryFuture() {
        SimpleRequest request = TestConnectorOuterClass.SimpleRequest.newBuilder()
                .setRequest("unary method request")
                .build();
        ListenableFuture<SimpleResponse> future = client.singleToSingleFuture(request);
        SimpleResponse response;
        try {
            response = future.get();
            Assertions.assertTrue(response.getResponse().contains("Response to"));
        } catch (InterruptedException | ExecutionException e) {
            Assertions.fail(e);
        }
    }


}
