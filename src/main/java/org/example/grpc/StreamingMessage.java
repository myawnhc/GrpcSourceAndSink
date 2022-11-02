package org.example.grpc;

import com.google.protobuf.GeneratedMessageV3;

import java.io.Serializable;
import java.util.UUID;

/**
 * This type is used as content of the Request and Response buffers when dealing
 * with streaming types.
 */
public class StreamingMessage<M> implements Serializable {

    MessageWithUUID<M> message;
    /**
     * Used to signal when stream has been marked completed via onComplete() call
     */
    boolean completed;

    public StreamingMessage(MessageWithUUID<M> message) {
        this.message = message;
        this.completed = false;
    }

    public StreamingMessage(MessageWithUUID<M> message, boolean complete) {
        this.message = message;
        this.completed = complete;
    }

    public MessageWithUUID<M> getMessage() { return message; }
    public boolean isComplete() { return completed; }
}
