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
