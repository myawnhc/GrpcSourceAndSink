#!/bin/bash

#
# /*
#  * Copyright (c) Hazelcast, Inc. 2022-2023.
#  *
#  *  Licensed under the Apache License, Version 2.0 (the "License");
#  *  you may not use this file except in compliance with the License.
#  *  You may obtain a copy of the License at
#  *
#  *    http://www.apache.org/licenses/LICENSE-2.0
#  *
#  *  Unless required by applicable law or agreed to in writing, software
#  *  distributed under the License is distributed on an "AS IS" BASIS,
#  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  *  See the License for the specific language governing permissions and
#  *  limitations under the License.
#  */
#
#

# TODO: Need 'with dependencies' versions of these if we're going to run from script rather than IDE
#   Actually if we do Tests with dependencies that should be all we need since it will pull in the connector
export CONNECTOR_JAR=target/GrpcSourceAndSink-1.0-SNAPSHOT.jar
export TEST_JAR=target/GrpcSourceAndSink-1.0-SNAPSHOT-tests.jar

java -cp $CONNECTOR_JAR:$TEST_JAR org.example.SampleService