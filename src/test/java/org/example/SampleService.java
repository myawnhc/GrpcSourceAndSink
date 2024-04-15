/*
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
 */

package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.grpc.stub.StreamObserver;
import org.example.grpc.APIBufferPair;
import org.example.grpc.Arity;
import org.example.grpc.GrpcServer;
import org.example.grpc.MessageWithUUID;
import org.example.grpc.StreamingMessage;
import org.hazelcast.grpcconnector.ExamplesGrpc;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.*;

public class SampleService extends ExamplesGrpc.ExamplesImplBase {

    private final String serviceName;
    private final IMap<String, APIBufferPair> bufferPairsForAPI;

    public SampleService(HazelcastInstance hazelcast) {
        GrpcServer server = new GrpcServer(this, 50050);
        serviceName = bindService().getServiceDescriptor().getName();
        System.out.println("SampleService starting to serve requests for " + serviceName);
        bufferPairsForAPI = hazelcast.getMap(serviceName+"_APIS");

        APIBufferPair<SimpleRequest,SimpleResponse> unaryHandler =
                new APIBufferPair(hazelcast,"sayHello", Arity.UNARY, Arity.UNARY);
        bufferPairsForAPI.put("sayHello", unaryHandler);

        APIBufferPair<StreamingMessage<RequestWithValue>,StreamingMessage<ResponseWithValue>>
                clientStreamingHandler = new APIBufferPair(hazelcast, "add",
                Arity.VARYING, Arity.UNARY);
        bufferPairsForAPI.put("add", clientStreamingHandler);

        APIBufferPair<MessageWithUUID<SimpleRequest>,StreamingMessage<SimpleResponse>>
                serverStreamingHandler = new APIBufferPair(hazelcast, "subscribe",
                Arity.UNARY, Arity.VARYING);
        bufferPairsForAPI.put("subscribe", serverStreamingHandler);

        APIBufferPair<ChatMessage, ChatMessage> bidirectionalStreamingHandler =
                new APIBufferPair(hazelcast, "chat", Arity.VARYING, Arity.VARYING);
        bufferPairsForAPI.put("chat", bidirectionalStreamingHandler);
    }

    @Override
    public void sayHello(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        APIBufferPair<SimpleRequest,SimpleResponse> unaryHandler = bufferPairsForAPI.get("sayHello");
        UUID identifier = UUID.randomUUID();
        unaryHandler.putUnaryRequest(identifier, request);
        SimpleResponse response = (SimpleResponse) unaryHandler.getUnaryResponse(identifier);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<RequestWithValue> add(StreamObserver<ResponseWithValue> responseObserver) {
        APIBufferPair<RequestWithValue,ResponseWithValue> bufferPair = bufferPairsForAPI.get("add");
        StreamObserver<RequestWithValue> requestObserver = new StreamObserver<>() {
            UUID clientIdentifier = UUID.randomUUID();
            @Override
            public void onNext(RequestWithValue request) {
                 bufferPair.putStreamingRequest(clientIdentifier, request);
            }

            @Override
            public void onError(Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {
                RequestWithValue emptyMessage = RequestWithValue.newBuilder().getDefaultInstanceForType();
                bufferPair.markRequestStreamComplete(clientIdentifier, emptyMessage);

                // Once completion signaled, we expect response to be available
                ResponseWithValue response = bufferPair.getUnaryResponse(clientIdentifier);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
        return requestObserver;
    }

    @Override
    public void subscribe(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        APIBufferPair<SimpleRequest,SimpleResponse> serverStreamingBufferPair = bufferPairsForAPI.get("subscribe");
        UUID identifier = UUID.randomUUID();
        serverStreamingBufferPair.putUnaryRequest(identifier, request);
        while (true) {
            StreamingMessage<SimpleResponse> response = serverStreamingBufferPair.getStreamingResponse(identifier.toString(), true);
            if (response.isComplete()) {
                responseObserver.onCompleted();
                serverStreamingBufferPair.acknowledgeResponseStreamCompletion(identifier.toString());
                break;
            } else {
                responseObserver.onNext(response.getMessage());
            }
        }
    }

    final AtomicInteger responsesSeen = new AtomicInteger();
    final AtomicInteger streamsMarkedComplete = new AtomicInteger();

    @Override
    public StreamObserver<ChatMessage> chat(final StreamObserver<ChatMessage> outboundChatObserver) {
        final APIBufferPair<ChatMessage, ChatMessage> bidiStreamingBufferPair = bufferPairsForAPI.get("chat");
        // Unlike others, we don't generate a random UUID here, as it is already set in the messages
        StreamObserver<ChatMessage> inboundChatObserver = new StreamObserver<>() {
            private String sender = null;

            @Override
            public void onNext(ChatMessage chatMessage) {
                if (sender == null)
                    sender = chatMessage.getSenderID();
                //String receiver = chatMessage.getReceiverID();
                bidiStreamingBufferPair.putStreamingRequest(UUID.fromString(sender), chatMessage);
            }

            @Override
            public void onError(Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {
                // We don't actually need to mark stream as complete as nothing is triggered by the
                // completion ... but we do want to release resources at some point.  Perhaps we send
                // completion, but filter it out of the pipeline?
                ChatMessage empty = ChatMessage.getDefaultInstance();
                // TODO: Single observer is shared across senders, so we don't know who marked us
                //  complete.   Seems perhaps we should only have a single onComplete after all
                //  senders are done?
                bidiStreamingBufferPair.markRequestStreamComplete(UUID.fromString(sender), empty);
                System.out.println("inbound (request) chat complete, marking for reclaim-after-receipt");
            }
        };

        // We need to start a separate thread to watch for responses; currently we use just one thread to
        // cycle through all active streams but if we needed to scale up we could have concurrent threads
        // per active chat connection
        Executors.newSingleThreadExecutor().submit(() -> {
            boolean startClosingStreams = false;
            boolean haveSeenActiveStreams = false;
            while (startClosingStreams == false) {
                Set<String> active = bidiStreamingBufferPair.getActiveResponseStreams("chat");
//                System.out.println("@top: Active streams " + active.size());
//                if (active.size() > 0)
//                    haveSeenActiveStreams = true;
                // We don't have a 100% reliable way of detecting when a stream can be closed; senders tell
                // us when they're done, but as long as there are senders left any receiver could be the
                // target.  So we just hold all queues until out sender count drops to zero.
                for (String identifier : active) {
                    StreamingMessage<ChatMessage> response = bidiStreamingBufferPair.getStreamingResponse(identifier, false);
                    if (response == null)
                        continue;
                    if (! response.isComplete()) {
                        System.out.println("SampleService.chat runnable: Sending chat response " + responsesSeen.incrementAndGet() + " for id " + identifier);
                        outboundChatObserver.onNext(response.getMessage());
                    } else {
                        outboundChatObserver.onCompleted();
                        int completeStreams = streamsMarkedComplete.incrementAndGet();
                        System.out.println("SampleService.chat runnable: Marking response complete: " + identifier + " #" + completeStreams + " of " + active.size());
                        if (completeStreams == active.size()) {
                            System.out.println("SampleService.chat runnable: All streams have been marked complete, begin freeing them");
                            startClosingStreams = true;
                        }
                    }
                    if (startClosingStreams)
                       break;

//                    if (startClosingStreams) {
//                        System.out.println("SampleService.chat runnable: closing stream " + identifier);
//                        bidiStreamingBufferPair.acknowledgeResponseStreamCompletion(identifier);
//                    }
                }
            }
            // Close streams
            Set<String> active = bidiStreamingBufferPair.getActiveResponseStreams("chat");
            System.out.println("SampleService.chat runnable exited loop, will close " + active.size() + " streams");
            for (String identifier : active) {
                System.out.println("SampleService.chat runnable: closing stream " + identifier);
                bidiStreamingBufferPair.acknowledgeResponseStreamCompletion(identifier);
            }
            Set<String> left = bidiStreamingBufferPair.getActiveResponseStreams("chat");
            System.out.println("  after closing completed streams, still have " + left.size() + " showing active");
        });
        return inboundChatObserver;
    }


    // Starts GRPC server
    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        SampleService service = new SampleService(hazelcast);
    }
}
