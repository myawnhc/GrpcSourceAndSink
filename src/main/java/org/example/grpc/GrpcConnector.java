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

package org.example.grpc;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class GrpcConnector implements HazelcastInstanceAware {

    private static final Logger logger = Logger.getLogger(GrpcConnector.class.getName());
    private HazelcastInstance hazelcast;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }


    // patterned off of https://docs.hazelcast.com/hazelcast/5.2/pipelines/custom-stream-source
    public static <R> StreamSource<MessageWithUUID<R>> grpcUnarySource(String serviceName, String apiName) {
        return SourceBuilder
                .stream("grpc-unary-source", ctx -> new GrpcContext<R,Object>(ctx.hazelcastInstance(), serviceName, apiName))
                .<MessageWithUUID<R>>fillBufferFn((grpcContext, sourceBuffer) -> {
                    int messagesAdded = 0;
                    MessageWithUUID<R> message = grpcContext.readUnaryRequest();
                    while (message != null) {
                        sourceBuffer.add(message);
                        if (messagesAdded++ > 100)
                            break;
                        message = grpcContext.readUnaryRequest();
                    }
                })
                .destroyFn(GrpcContext::close)
                .build();
    }

    public static <R> StreamSource<StreamingMessage<R>> grpcStreamingSource(String serviceName, String apiName) {
        return SourceBuilder
                .stream("grpc-streaming-source", ctx -> new GrpcContext<R,Object>(ctx.hazelcastInstance(), serviceName, apiName))
                .<StreamingMessage<R>>fillBufferFn((grpcContext, sourceBuffer) -> {
                    int messagesAdded = 0;
                    List<StreamingMessage<R>> messages = grpcContext.readStreamingRequests();
                    while (!messages.isEmpty()) {
                        for (StreamingMessage<R> message : messages) {
                            sourceBuffer.add(message);
                            messagesAdded++;
                        }
                        if (messagesAdded > 100)
                            break;
                        // Get another batch until we fill the buffer
                        messages = grpcContext.readStreamingRequests();
                    }
                })
                .destroyFn(GrpcContext::close)
                .build();
    }

    public static <R> Sink<R> grpcUnarySink(String serviceName, String apiName) {
        return SinkBuilder.sinkBuilder(
                        "grpc-unary-sink", pctx -> new GrpcContext<Object,R>(pctx.hazelcastInstance(), serviceName, apiName))
                .<R>receiveFn((writer, item) -> {
                    writer.writeUnaryResponse((MessageWithUUID<R>) item);
                })
                .destroyFn(GrpcContext::close)
                .build();
    }

    public static <R> Sink<R> grpcStreamingSink(String serviceName, String apiName) {
        return SinkBuilder.sinkBuilder(
                        "grpc-streaming-sink", pctx -> new GrpcContext<Object,R>(pctx.hazelcastInstance(), serviceName, apiName))
                .<R>receiveFn((writer, item) -> {
                    writer.writeStreamingResponse((StreamingMessage<R>) item);
                })
                .destroyFn(GrpcContext::close)
                .build();
    }


    private static class GrpcContext<REQ, RESP> {
        private final APIBufferPair<REQ,RESP> bufferPair;
        private String apiName;

        public GrpcContext(HazelcastInstance hazelcast, String serviceName, String apiName)  {
            this.apiName = apiName;
            IMap<String, APIBufferPair<REQ,RESP>> handlers = hazelcast.getMap(serviceName+"_APIS");
            bufferPair = handlers.get(apiName);
        }

        public void close() {
            // nop
        }

        public MessageWithUUID<REQ> readUnaryRequest()  {
            return bufferPair.getUnaryRequest();
        }

        public void writeUnaryResponse(MessageWithUUID<RESP> item) {
            bufferPair.putUnaryResponse(item);
        }

        public List<StreamingMessage<REQ>> readStreamingRequests() {
            List<StreamingMessage<REQ>> results = new ArrayList<>();
            // Returns at most one item per identifier; if fillBufferFn wants more
            // it will call us again.
            for (String identifier : bufferPair.getActiveRequestStreams(apiName)) {
                // Active doesn't necessarily mean ready, so we can get null here
                StreamingMessage<REQ> message = bufferPair.getStreamingRequest(identifier);
                if (message != null) {
                    if (message.isComplete()) {
                        results.add(message); // discuss.
                        bufferPair.acknowledgeRequestStreamCompletion(identifier);
                    } else {
                        results.add(message);
                    }
                }

            }
            return results;
        }

        public void writeStreamingResponse(StreamingMessage<RESP> streamingMessage) {
            UUID identifier = streamingMessage.getIdentifier();
            RESP message = streamingMessage.getMessage(); // will be empty if completed true
            boolean completed = streamingMessage.isComplete();
            if (completed) {
                logger.info("GrpcContext.writeStreamingResponse marks " + identifier + " complete");
                bufferPair.markResponseStreamComplete(identifier, message);
            } else {
                bufferPair.putStreamingResponse(identifier, message);
            }
        }
    }
}

