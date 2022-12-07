package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import io.grpc.stub.StreamObserver;
import org.example.grpc.APIBufferPair;
import org.example.grpc.Arity;
import org.example.grpc.GrpcServer;
import org.example.grpc.MessageWithUUID;
import org.example.grpc.StreamingMessage;
import org.hazelcast.grpcconnector.ExamplesGrpc;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ResponseWithValue;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.ChatMessage;


public class SampleService extends ExamplesGrpc.ExamplesImplBase {

    private final String serviceName;
    private final IMap<String, APIBufferPair> bufferPairsForAPI; // In process of deprecation

    private final IExecutorService distributedExecutor;

    public SampleService(HazelcastInstance hazelcast) {
        GrpcServer server = new GrpcServer(this, 50050);
        serviceName = bindService().getServiceDescriptor().getName();
        System.out.println("SampleService starting to serve requests for " + serviceName);
        bufferPairsForAPI = hazelcast.getMap(serviceName+"_APIS");
        distributedExecutor = hazelcast.getExecutorService("SampleService executor");

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
                System.out.println("Server side: Request stream marked complete, fetching response");

                // Once completion signaled, we expect response to be available
                System.out.println("Getting accumulated response - may block ");
                ResponseWithValue response = bufferPair.getUnaryResponse(clientIdentifier);
                System.out.println("Server side: Response prior to sending to client" + response);
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
                System.out.println("Marking subscription response complete");
                responseObserver.onCompleted();
                serverStreamingBufferPair.acknowledgeResponseStreamCompletion(identifier.toString());
                break;
            } else {
                System.out.println("Sending subscription response");
                responseObserver.onNext(response.getMessage());
            }
        }
    }

    // Separate runnable to collect responses coming back to us from the chat service
    class ChatObserver implements Runnable, Serializable {
        final APIBufferPair<ChatMessage,ChatMessage> bidiStreamingBufferPair = bufferPairsForAPI.get("chat");
        final StreamObserver<ChatMessage> outboundChatObserver;

        public ChatObserver(StreamObserver<ChatMessage> observer) {
            outboundChatObserver = observer;
        }
        @Override
        public void run() {
            // TODO: this never terminates ... maybe if we know number of chats started we can
            //  count how many have completed in the isComplete block.
            int responsesSeen = 0;
            int observersMarkedComplete = 0;
            while (true) {
                // DEBUG
                // Sequence seen: 1-2-6 repeats until something becomes active;
                //  then 1-2-2-3-2-3-3-4-4-4 which doesn't seem like a possible outcome.
                // Seems like we hang at the point that a second stream is marked active but that could
                // just be a coincidence
                System.out.println("Runnable 1");
                Set<String> active = bidiStreamingBufferPair.getActiveResponseStreams("chat");
                System.out.println("Runnable 2 ... active response stream count " + active.size());
                // ~DEBUG
                for (String identifier : active) {
                    System.out.println("Runnable 3");
                    StreamingMessage<ChatMessage> response = bidiStreamingBufferPair.getStreamingResponse(identifier, false);
                    System.out.println("Runnable 4");
                    if (response.isComplete()) {
                        System.out.println("Marking chat response complete: " + identifier);
                        outboundChatObserver.onCompleted();
                        observersMarkedComplete++;
                        // TODO: may at least temporarily remove this as sender and receiver onComplete are not correlated!
                        //  - but so far we aren't even receiving the completion message so there's an upstream issue.
                        // TODO: 'markComplete' and 'acknowledge' should happen at different sides on the connection,
                        //   verify that connector is calling markStreamComplete
                        System.out.println("... and reclaiming resources");
                        bidiStreamingBufferPair.acknowledgeResponseStreamCompletion(identifier);
                        //break;
                    } else {
                        System.out.println("Service: Sending chat response " + ++responsesSeen + " for id " + identifier);
                        outboundChatObserver.onNext(response.getMessage());
                    }
                    System.out.println("Runnable 5");
                }
                System.out.println("Runnable 6");
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
                //System.out.println("inbound (request) chat complete, marking for reclaim-after-receipt");
            }
        };
        // We need to start a separate thread to watch for responses; currently we use just one thread to
        // cycle through all active streams but if we needed to scale up we could have concurrent threads
        // per active chat connection
        // TODO: note that this is getting called on multiple threads -- we are accomplishing nothing
        // by declaring a new singleThreadExecutor here, as it just creates a new STE on each
        // thread.  If we really need to be single-threaded then we need to move the executor
        // creation out to service scope rather than do it in a method that can be called on any thread.
        Executors.newSingleThreadExecutor().submit(() -> {
            boolean startClosingStreams = false;
            while (true) {
                Set<String> active = bidiStreamingBufferPair.getActiveResponseStreams("chat");
                // We don't have a 100% reliable way of detecting when a stream can be closed; senders tell
                // us when they're done, but as long as there are senders left any receiver could be the
                // target.  So we just hold all queues until out sender count drops to zero.
                for (String identifier : active) {
                    StreamingMessage<ChatMessage> response = bidiStreamingBufferPair.getStreamingResponse(identifier, false);
                    if (response == null)
                        continue;
                    //System.out.println("Runnable: active response stream count " + active.size());
                    //String sender = response.getMessage().getSenderID();
                    //String receiver = response.getMessage().getReceiverID();
                    //boolean complete = response.isComplete();
                    //System.out.println("Runnable 4 for identifier " + identifier + " details: from " + sender + " to " + receiver + " complete? " + complete);
                    if (response.isComplete()) {
                        outboundChatObserver.onCompleted();
                        int completeStreams = streamsMarkedComplete.incrementAndGet();
                        System.out.println("Runnable: Marking response complete: " + identifier + " #" + completeStreams + " of " + active.size());
                        if (completeStreams == active.size()) {
                            System.out.println("Runnable: All streams have been marked complete, begin freeing them");
                            startClosingStreams = true;
                        }
                    } else {
                        System.out.println("Runnable: Sending chat response " + responsesSeen.incrementAndGet() + " for id " + identifier);
                        outboundChatObserver.onNext(response.getMessage());
                    }
                    if (startClosingStreams) {
                        System.out.println("Runnable: Closing stream " + identifier);
                        bidiStreamingBufferPair.acknowledgeResponseStreamCompletion(identifier);
                    }
                }
            }
        });
        return inboundChatObserver;
    }


    // Starts GRPC server
    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        SampleService service = new SampleService(hazelcast);
        // Run until terminated
        while(true)
            MINUTES.sleep(60);
    }
}
