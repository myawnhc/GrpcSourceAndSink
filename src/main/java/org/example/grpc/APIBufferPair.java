package org.example.grpc;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class APIBufferPair<REQ, RESP> implements Serializable, HazelcastInstanceAware {

    private final String methodName;
    transient private IQueue<REQ> requests;
    transient private IMap<UUID,RESP> responses;

    public APIBufferPair(HazelcastInstance hazelcast, String methodName) {
        this.methodName = methodName;
        requests = hazelcast.getQueue(methodName+"_Requests");
        responses = hazelcast.getMap(methodName+"_Responses");
    }

    public REQ getRequest() {
        try {
            //System.out.println("Reading request from " + requests.getName());
            return requests.poll(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null; // This will not end well, should probably just rethrow
        }
    }

    public void putRequest(REQ request)  {
        System.out.println("Writing request " + request.toString());
        try {
            requests.put(request);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void putResponse(MessageWithUUID<RESP> response) {
        System.out.println("Writing response " + response.toString() + " to map " + responses.getName());
        UUID identifier = response.getIdentifier();
        responses.put(identifier, response.getMessage());
    }

    public RESP getResponse(UUID identifier) {
        System.out.println("Reading response from " + responses.getName() + " for id " + identifier);
        // TODO: need to be able to wait for response to get populated -- may resurrect
        //   waitForResponse ... could either sleep or arm an EntryListener, but for now
        //   just doing a tight loop
        RESP response = null;
        while (response == null) {
            response = responses.get(identifier);
            // sleep?
        }
        System.out.println("Read response " + response + " from " + responses.getName() + " for id " + identifier);
        return response;
    }

//    public RESP waitForResponse() {
//        System.out.println("Waiting for response from " + responses.getName());
//        try {
//            return responses.take();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            return null;
//        }
//    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcast) {
        requests = hazelcast.getQueue(methodName+"_Requests");
        responses = hazelcast.getMap(methodName+"_Responses");
    }
}
