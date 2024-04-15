# GrpcSourceAndSink
A Jet Connector for gRPC as a Source and Sink

This README is still under development

This proof of concept provides a gRPC Source and Sink connector for gRPC to allow 
the Hazelcast Platform (specifically the Jet Stream Processing engine) to implement gRPC-defined
services.

Unlike most Sources and Sinks, which are independent of each other,  these will operate in pairs - 
a pipeline will read a 
gRPC-defined Request from the Source, process it, and write the Response back to the paired
Sink. 

## Features

gRPC supports both streaming and unary (single-valued) requests and responses, giving a total of 4 possible combinations:
- Unary request, Unary response
- Unary request, Streaming response (aka Server streaming)
- Streaming request, Unary response (aka Client streaming)
- Streaming request, Streaming response (aka Bidirectional streaming)

The gRPC source and sink is designed to support all 4 of these combinations.  (Currently the unit test for the final option, bidirectional streaming, is failing so that option may not be working, although an error in the test itself is suspected).

gRPC also supports different interaction modes - blocking requests, non-blocking (async) requests (response will be sent to a provided callback function), and async requests that return a Future.  This framework supports all of these interaction styles.  (Not all styles are supported by all of the request-response type combinations, but no additional constraints are imposed by the framework compared to what gRPC supports).

## Components

The GrpcConnector class has the Source and Sink implementation that will be used in Jet pipelines.  The pipeline readFrom() stage can read from either a <b>grpcUnarySource</b> or a <b>grpcStreamingSource</b>.  When the pipeline has complted, the writeTo() stage can send the result to either a <b>grpcUnarySink</b> or a <b>grpcStreamingSink</b>.

When implementing a gRPC server, there will be one or more classes that implement the proto APIs defined by the gRPC .proto file.  These implementation classes will take requests coming from the gRPC clients and write them to the input side of an APIBufferPair; they will then read the resulting response from the output side of the APIBufferPair. 

## Building 

Use maven to build the project. 

## Using

The Examples.proto file under test/proto shows the example services that are used for testing the framework.  There is one example for each request-response type.  

The implementation of the server-side stubs is in org.example.SampleService in the test/java directory.  The actual processing is done by the Jet pipelines in the SamplePipelines class. 

A more detailed how-to-use guide is needed and will be added to this document or (more likely) a separate developer's document. 

## Testing

Unit tests here are not standalone; in order to run them do the following (all referenced classes are in org.example package under test directory)
- Start the SampleService (starts a Hazelcast cluster member and a GRPC Server)
- Start the SamplePipelines (will submit multiple jobs to the Hazelcast cluster)
- Run APIUnitTests 
  
### Current Testing issues
The Bidirectional Streaming unit passes on initial run, but will fail if re-run without restarting
the service. Suspect that some clean-up isn't happening correctly but have not isolated the issue. 


