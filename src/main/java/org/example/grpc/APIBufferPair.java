package org.example.grpc;

import com.google.protobuf.GeneratedMessageV3;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class APIBufferPair<REQ extends GeneratedMessageV3, RESP extends GeneratedMessageV3>
            implements Serializable, HazelcastInstanceAware {

    private final String methodName;
    transient private IQueue<REQ> requests;
    transient private IQueue<RESP> responses;

    public APIBufferPair(HazelcastInstance hazelcast, String methodName) {
        this.methodName = methodName;
        requests = hazelcast.getQueue(methodName+"Requests");
        responses = hazelcast.getQueue(methodName+"Responses");
    }

    public REQ getRequest() {
        try {
            //System.out.println("Reading " + requests.getName());
            return requests.poll(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("No requests in queue, returning null");
            return null;
        }
    }

    public void putRequest(REQ request)  {
        try {
            requests.put(request);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void putResponse(RESP response) {
        try {
            responses.put(response);
        } catch (InterruptedException e) {
            e.printStackTrace();
         }
    }
    public RESP getResponse() {
        try {
            return responses.poll(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcast) {
        System.out.println("APIHandler initialized with HazelcastInstance");
        requests = hazelcast.getQueue(methodName+"Requests");
        responses = hazelcast.getQueue(methodName+"Responses");
    }
}
