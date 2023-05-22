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

package org.example;

import io.grpc.stub.StreamObserver;
import org.example.grpc.GrpcServer;
import org.hazelcast.grpcconnector.ExamplesGrpc;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleRequest;
import static org.hazelcast.grpcconnector.ExamplesOuterClass.SimpleResponse;

public class SampleNonHZService extends ExamplesGrpc.ExamplesImplBase {

    public SampleNonHZService() {
        GrpcServer server = new GrpcServer(this, 50050);
    }

    @Override
    public void sayHello(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
        SimpleResponse response = SimpleResponse.newBuilder()
                .setResponse("Hello, " + request.getRequest())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    // Starts GRPC server
    public static void main(String[] args) throws InterruptedException {
        SampleNonHZService service = new SampleNonHZService();
        // Run until terminated
        while(true)
            MINUTES.sleep(60);
    }
}
