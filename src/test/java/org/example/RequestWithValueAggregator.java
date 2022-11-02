/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import java.io.Serializable;
import java.util.UUID;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.Tuple3;

import static org.hazelcast.grpcconnector.ExamplesOuterClass.RequestWithValue;

/**  Accumulate the input values of RequestWithValue messages
 */
public class RequestWithValueAggregator implements Serializable {
    private static final long serialVersionUID = 1L;
    // Don't log as may run in the cloud
    // private static final Logger LOGGER = LoggerFactory.getLogger(MaxVolumeAggregator.class);

    private UUID clientIdentifier;
    private int accumulatedValue;
    private boolean completed;

    public static AggregateOperation1<Tuple3<UUID, RequestWithValue, Boolean>,
            RequestWithValueAggregator, Tuple3<UUID,RequestWithValue, Boolean>> build() {
        return AggregateOperation
                .withCreate(RequestWithValueAggregator::new)
                .andAccumulate((RequestWithValueAggregator requestWithValueAggregator,
                                Tuple3<UUID, RequestWithValue, Boolean> entry)
                        -> requestWithValueAggregator.accumulate(entry))
                .andCombine(RequestWithValueAggregator::combine)
                .andExportFinish(RequestWithValueAggregator::exportFinish);
    }

    public RequestWithValueAggregator() {
        this.clientIdentifier = null;
        this.accumulatedValue = 0;
        this.completed = false;
    }

    /**  Update the accumulated value by adding the new input value
     *
     * @param tuple3 of clientID, RequestWithValue message, and completion flag
     * @return The current accumulator
     */
    public RequestWithValueAggregator accumulate(Tuple3<UUID,RequestWithValue, Boolean> tuple3) {
        clientIdentifier = tuple3.f0();
        if (tuple3.f1() != null) {
            int valueToAdd = tuple3.f1().getInputValue();
            accumulatedValue += valueToAdd;
        }
        completed = tuple3.f2();
        return this;
    }

    /**
     * Combine with another accumulator (from another node or partition)
     */
    public RequestWithValueAggregator combine(RequestWithValueAggregator that) {
        assert(clientIdentifier.equals(that.clientIdentifier));
        accumulatedValue += that.accumulatedValue;
        completed = completed || that.completed;
        return this;
    }

    /** Return the result
     */
    public Tuple3<UUID,RequestWithValue,Boolean> exportFinish() {
        RequestWithValue message = RequestWithValue.newBuilder()
                .setInputValue(accumulatedValue)
                .build();
        return Tuple3.tuple3(clientIdentifier, message, completed);
    }
}