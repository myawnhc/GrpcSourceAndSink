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

import com.google.protobuf.GeneratedMessageV3;

import java.io.Serializable;
import java.util.UUID;

/**
 * This type is used as content of the Request and Response buffers when dealing
 * with streaming types.
 */
public class StreamingMessage<M> implements Serializable {

    UUID identifier;
    M message;
    /**
     * Used to signal when stream has been marked completed via onComplete() call
     */
    boolean completed;

    public StreamingMessage(UUID identifier, M message, boolean complete) {
        this.identifier = identifier;
        this.message = message;
        this.completed = complete;
    }

    public UUID getIdentifier() { return identifier; }
    public M getMessage() { return message; }
    public boolean isComplete() { return completed; }
}
