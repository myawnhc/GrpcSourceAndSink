package org.example.grpc;

import com.google.protobuf.GeneratedMessageV3;

import java.io.Serializable;
import java.util.UUID;

public class MessageWithUUID<M> implements Serializable {
    /**
     * Unique identifier used to keep messages for different clients separate; they are
     * separated within the gRPC StreamObserver but when we add them to our APIBufferPair
     * object we risk interleaving messages from different clients.
     */
    private UUID identifier;
    private M message;
    public MessageWithUUID(UUID identifier, M message) {
        this.identifier = identifier;
        this.message = message;
    }
    public UUID getIdentifier() { return identifier; }
    public M getMessage() { return message; }
    public String toString() {
        return "[" + identifier.toString() + "]" + message;
    }
}
