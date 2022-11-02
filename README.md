# GrpcSourceAndSink
A Jet Connector for gRPC as a Source and Sink

Under development - check back later for useful README.

When fully complete, this will provide a gRPC Source and Sink connector for gRPC to allow 
the Hazelcast Platform (specifically the Jet Stream Processing engine) to implement gRPC-defined
services.

Unlike most Sources and Sinks, which are independent of each other,  these will operate in pairs - 
a pipeline will read a 
gRPC-defined Request from the Source, process it, and write the Response back to the paired
Sink. 
