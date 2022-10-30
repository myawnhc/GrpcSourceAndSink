package org.example.grpc;

import com.google.protobuf.GeneratedMessageV3;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import io.grpc.BindableService;

import java.util.logging.Logger;

public class GrpcConnector implements HazelcastInstanceAware {

    private static final Logger logger = Logger.getLogger(GrpcConnector.class.getName());
    private HazelcastInstance hazelcast;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    // patterned off of https://docs.hazelcast.com/hazelcast/5.2/pipelines/custom-stream-source
    public static <R extends GeneratedMessageV3> StreamSource<R> grpcSource(String serviceName, String apiName) {
        return SourceBuilder
                .stream("grpc-source", ctx -> new GrpcContext<R,R>(ctx.hazelcastInstance(), serviceName, apiName))
                .<R>fillBufferFn((grpcContext, sourceBuffer) -> {
                    int messagesAdded = 0;
                    R message = grpcContext.read();
                    while (message != null && messagesAdded++ < 100) {
                        sourceBuffer.add(message);
                        message = grpcContext.read();
                    }
                    if (sourceBuffer.size() > 0)
                        System.out.println("fillBufferFn added " + messagesAdded + " messages");
                })
                .destroyFn(GrpcContext::close)
                .build();
    }

    public static <R extends GeneratedMessageV3> Sink<Object> grpcSink(String serviceName, String apiName) {
        return SinkBuilder.sinkBuilder(
                        "grpc-sink", pctx -> new GrpcContext<R,R>(pctx.hazelcastInstance(), serviceName, apiName))
                .receiveFn((writer, item) -> {
                    // TODO: we may pass a tuple - completion flag, item
                    writer.write((R) item);
                    // TODO: if streaming this is conditional .
                    writer.markComplete();
                })
                .destroyFn(GrpcContext::close)
                .build();
    }

    private static class GrpcContext<REQ extends GeneratedMessageV3, RESP extends GeneratedMessageV3> {
        //private String serviceName;
        private final APIBufferPair<REQ,RESP> apiHandler;

        public GrpcContext(HazelcastInstance hazelcast, String serviceName, String apiName)  {
            IMap<String, APIBufferPair<REQ,RESP>> handlers = hazelcast.getMap(serviceName+"_APIS");
            apiHandler = handlers.get(apiName);
        }

        public void close() {
            // nop
        }

        public REQ read()  {
            return apiHandler.getRequest();
        }

        public boolean isComplete() {
            // TODO: for streaming requests
            return false;
        }

        public void write(RESP item) {
            apiHandler.putResponse(item);
        }

        public void markComplete() {
            // TODO: for streaming responses
        }
    }

}

