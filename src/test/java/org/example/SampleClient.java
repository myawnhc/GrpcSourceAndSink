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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.hazelcast.grpcconnector.ExamplesGrpc;

import java.util.Iterator;
import java.util.logging.Logger;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ResponseWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ChatMessage;

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

    public void subscribeAsync(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        // This is not available via the future stub
        asyncStub.subscribe(request, responseObserver);
    }

    public Iterator<SimpleResponse> subscribeBlocking(SimpleRequest request) {
        // This is not available via the future stub
        return blockingStub.subscribe(request);
    }

    /** Returns request observer that caller will use to send */
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver) {
        // This is not available via the future or blocking stubs
        return asyncStub.chat(responseObserver);
    }
}
