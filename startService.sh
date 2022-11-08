#!/bin/bash

# TODO: Need 'with dependencies' versions of these if we're going to run from script rather than IDE
#   Actually if we do Tests with dependencies that should be all we need since it will pull in the connector
export CONNECTOR_JAR=target/GrpcSourceAndSink-1.0-SNAPSHOT.jar
export TEST_JAR=target/GrpcSourceAndSink-1.0-SNAPSHOT-tests.jar

java -cp $CONNECTOR_JAR:$TEST_JAR org.example.SampleService