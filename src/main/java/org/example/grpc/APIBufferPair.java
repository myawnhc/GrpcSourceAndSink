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

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/** A buffer pair is used to associate data being passed into a gRPC source with the
 *  result that will be returned via a gRPC sink.
 *
 * @param <REQ>  Type of request object
 * @param <RESP> Type of response object
 */
public class APIBufferPair<REQ, RESP> implements Serializable, HazelcastInstanceAware {

    private transient HazelcastInstance hazelcast;
    private Arity requestArity;
    private Arity responseArity;
    private final String methodName;
    transient private IQueue<MessageWithUUID<REQ>> unaryRequests;
    transient private IMap<UUID,RESP> unaryResponses;
    // We don't hold references to streaming request/response queues, we get them from hazelcast as needed
    //transient private IQueue<String> streamingRequestIDs;
    transient private IMap<String,Set<String>> activeRequestStreams;
    transient private IMap<String,Set<String>> activeResponseStreams;

    private static final Logger logger = Logger.getLogger(APIBufferPair.class.getName());

    public APIBufferPair(HazelcastInstance hazelcast, String methodName, Arity requestArity, Arity responseArity) {
        this.hazelcast = hazelcast;
        this.methodName = methodName;
        this.requestArity = requestArity;
        this.responseArity = responseArity;
        initHazelcastStructures();
    }

    private void initHazelcastStructures() {
        if (requestArity == Arity.UNARY)
            unaryRequests = hazelcast.getQueue(methodName+"_Requests");
        if (responseArity == Arity.UNARY)
            unaryResponses = hazelcast.getMap(methodName+"_Responses");
        activeRequestStreams = hazelcast.getMap("activeRequestStreams");
        activeResponseStreams = hazelcast.getMap("activeResponseStreams");
    }

    /** This will return the next request, if one is available; if there is no
     * pending request it will return null.  (Only caller is expected to be
     * GrpcSource fillBufferFn, which expects data to not always be ready)
     *
     * @return next queued request, or null if no requests are queued
     */
    public MessageWithUUID<REQ> getUnaryRequest() {
        //Called by pipeline very frequently if awaiting data, so this is very noisy to log.
        //logger.info("Reading unary request from " + unaryRequests.getName());
        return unaryRequests.poll();
    }

    /** Called by the service to queue up a request for the pipeline */
    public void putUnaryRequest(UUID identifier, REQ request)  {
        try {
            MessageWithUUID<REQ> wrappedMessage = new MessageWithUUID<>(identifier, request);
            unaryRequests.put(wrappedMessage); // may block
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void putUnaryResponse(MessageWithUUID<RESP> response) {
        //System.out.println("Writing unary response " + response.toString() + " to map " + unaryResponses.getName());
        UUID identifier = response.getIdentifier();
        unaryResponses.put(identifier, response.getMessage());
    }

    class UnaryResponseListener implements EntryAddedListener<UUID, RESP> {
        private CompletableFuture<RESP> future;

        public UnaryResponseListener(CompletableFuture<RESP> future) {
            this.future = future;
        }
        @Override
        public void entryAdded(EntryEvent<UUID, RESP> entryEvent) {
            future.complete(entryEvent.getValue());
            unaryResponses.remove(entryEvent.getKey());
            //logger.info("UnaryResponse added for " + entryEvent.getKey() + ", future completed");
        }
    }

    /** This call will return a response once ready.
     *
     * @param identifier identifier used to pair the response with the request
     * @return
     */
    public RESP getUnaryResponse(UUID identifier) {
        CompletableFuture<RESP> future = new CompletableFuture<RESP>();
        UnaryResponseListener listener = new UnaryResponseListener(future);
        UUID removalKey = unaryResponses.addEntryListener(listener, identifier, true);
        try {
            // Possible that a response arrived before our listener was armed
            RESP value = unaryResponses.remove(identifier);
            if (value != null) {
                //logger.info("UnaryResponse was already set for " + identifier + " removing response and (superfluous) listener");
                return value;
            } else {
                return future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            unaryResponses.removeEntryListener(removalKey);
        }
    }

    public void putStreamingRequest(UUID identifier, REQ request) {
        //logger.info("Writing streaming request " + request.toString() + " for id " + identifier);
        activeRequestStreams.executeOnKey(methodName, (EntryProcessor<String, Set<String>, Object>) entry -> {
            Set<String> streamsForAPI = entry.getValue();
            if (streamsForAPI == null) {
                streamsForAPI = new HashSet<>();
                //System.out.println("Initialized active request streams set for " + methodName);
            }
            if (!streamsForAPI.contains(identifier.toString())) {
                streamsForAPI.add(identifier.toString());
                entry.setValue(streamsForAPI);
                //System.out.println("Updated active request streams for " + methodName + " to add " + identifier);
            }
            return null;
        });
        IQueue<StreamingMessage<REQ>> requestStream = hazelcast.getQueue(identifier.toString()+"_Requests");
        StreamingMessage<REQ> wrappedMessage = new StreamingMessage(identifier, request, false);
        requestStream.add(wrappedMessage);
    }

    public void putStreamingResponse(UUID identifier, RESP response) {
        //logger.info("Writing streaming response for id " + identifier);
        activeResponseStreams.executeOnKey(methodName, (EntryProcessor<String, Set<String>, Object>) entry -> {
            Set<String> streamsForAPI = entry.getValue();
            if (streamsForAPI == null) {
                streamsForAPI = new HashSet<>();
                System.out.println("Initialized active response streams set for " + methodName);
            }
            if (!streamsForAPI.contains(identifier.toString())) {
                streamsForAPI.add(identifier.toString());
                entry.setValue(streamsForAPI);
                System.out.println("Updated active response streams for " + methodName + " to add " + identifier);
            }
            return null;
        });

        IQueue<StreamingMessage<RESP>> responseStream = hazelcast.getQueue(identifier.toString()+"_Responses");
        StreamingMessage<RESP> wrappedMessage = new StreamingMessage(identifier, response, false);
        responseStream.add(wrappedMessage);
    }

    public Set<String> getActiveRequestStreams(String methodName) {
        Set<String> streams = activeRequestStreams.get(methodName);
        return streams == null ? Collections.emptySet() : streams;
    }

    public Set<String> getActiveResponseStreams(String methodName) {
        Set<String> streams = activeResponseStreams.get(methodName);
        return streams == null ? Collections.emptySet() : streams;
    }

    public StreamingMessage<REQ> getStreamingRequest(String identifier) {
        IQueue<StreamingMessage<REQ>> requestStream = hazelcast.getQueue(identifier + "_Requests");
        return requestStream.poll();  // We want non-blocking here .. fillBufferFn shouldn't block
    }

    public StreamingMessage<RESP> getStreamingResponse(String identifier, boolean blocking) {
        IQueue<StreamingMessage<RESP>> responseStream = hazelcast.getQueue(identifier + "_Responses");
        try {
            if (blocking)  // Common case - client generally blocks until response is ready
                return responseStream.take();
            else
                return responseStream.poll(); // used when client monitoring multiple response streams
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void markRequestStreamComplete(UUID identifier, REQ emptyMessage) {
        StreamingMessage<REQ> completion = new StreamingMessage<>(identifier, emptyMessage, true);
        IQueue<StreamingMessage<REQ>> requestStream = hazelcast.getQueue(identifier.toString() + "_Requests");
        requestStream.add(completion);
    }

    public void markResponseStreamComplete(UUID identifier, RESP emptyMessage) {
        StreamingMessage<RESP> completion = new StreamingMessage<>(identifier, emptyMessage, true);
        IQueue<StreamingMessage<RESP>> responseStream = hazelcast.getQueue(identifier.toString() + "_Responses");
        responseStream.add(completion);
    }

    /** Acknowledge when the completion message for a streaming type has been consumed so we can remove
     * the queue
     * @param identifier
     */
    public void acknowledgeRequestStreamCompletion(String identifier) {
        //logger.info("acknowledgeRequestStreamCompletion for " + identifier);
        activeRequestStreams.executeOnKey(methodName, (EntryProcessor<String, Set<String>, Object>) entry -> {
            Set<String> streamsForAPI = entry.getValue();
            if (streamsForAPI == null) {
                System.out.println("ActiveRequestStreams: Attempted to remove identifier " + identifier + " from empty set");
            }
            streamsForAPI.remove(identifier);
            // Set may now be empty, we'll keep the entry with empty set rather than null out the entry.
            entry.setValue(streamsForAPI);
            //System.out.println("Updated active request streams for " + methodName + " to remove " + identifier);
            return null;
        });
    }

    public void acknowledgeResponseStreamCompletion(String identifier) {
        //logger.info("acknowledgeResponseStreamCompletion for " + identifier);
        activeResponseStreams.executeOnKey(methodName, (EntryProcessor<String, Set<String>, Object>) entry -> {
            Set<String> streamsForAPI = entry.getValue();
            if (streamsForAPI == null) {
                System.out.println("ActiveResponseStreams: Attempted to remove identifier " + identifier + " from empty set");
            }
            streamsForAPI.remove(identifier);
            // Set may now be empty, we'll keep the entry with empty set rather than null out the entry.
            entry.setValue(streamsForAPI);
            System.out.println("Updated active response streams for " + methodName + " to remove " + identifier);
            return null;
        });
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        initHazelcastStructures();
    }
}
