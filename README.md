# Pyflink Examples using Docker

While examples and documentation for using pyflink, and setting it up with docker may be found [here](https://nightlies.apache.org/flink/flink-docs-stable/), [here](https://github.com/apache/flink/tree/release-1.14/flink-python/pyflink/examples) and [here](https://github.com/pyflink/playgrounds) (although only for Flink 1.13 at the time of writing) some may benefit from seeing more examples, including examples of additional use cases. 

This repository is intended to provide such further examples, however the repository makes no claim to represent best practices Pyflink practices.

The examples include standard examples, for example the word count example for streaming and batch using both the Datastream and Table APIs in a docker container.   
[Table API, Datastream API]

It also includes a version of the playground from [here](https://github.com/pyflink/playgrounds) with modifications to support Flink 14.4, as well as minor cosmetic changes.  
[Table API, UDF, Kafka, Elasticsearch, Kibana]

In addition, an example of combining the Table and Datastream API's to implement the keyed state function from [here](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/fault-tolerance/state/) is included.    
[Table API, Datastream API, Stateful Function, Kafka, Elasticsearch, Kibana]

Furthermore, a different keyed state function, analysing random 1's and 0's generated and sent with kafka to record the longest run of consecutive 1's and 0's and when it occurs is included.        
[Table API, Datastream API, Stateful Function, Kafka, Elasticsearch, Kibana]

A process where integers are being generated and then analysised for their primality is also included.   
[Table API, Datastream API, Stateful Function, Parallel Execution, Kafka, Elasticsearch, Kibana]