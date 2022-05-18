# Pyflink and Docker Examples
This repository concerns the Python interface of the [Flink](https://flink.apache.org/) Streaming Processing framework, called pyflink. A basic high level [overview of Pyflink](FlinkandPyflink.md), largely for personal reference, is included in the repository.

While examples and documentation for using pyflink with docker may be found [here](https://nightlies.apache.org/flink/flink-docs-stable/), [here](https://github.com/apache/flink/tree/release-1.14/flink-python/pyflink/examples) and [here](https://github.com/pyflink/playgrounds) (only for Flink 1.13 at the time of writing) some may benefit from seeing more examples, including examples of additional use cases. 

This repository is intended to provide such further examples, **the repository makes no claim to represent best Pyflink practices**.

## Examples
[(1)](example-pipeline) Large example using both API's and a variety of operations. Implements a simple prototype of a pipeline for quality assessment of a raio telescope data.        
**[Table API, Datastream API, Stateful Function, Kafka, Elasticsearch, Kibana]**

[(2)](wordcount) The [standard](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table_api_tutorial/) wordcount example for streaming and batch using both the datastream and table APIs in a docker container.   
**[Table API, Datastream API]**

[(3)](modified-playground) A version of the playground from [here](https://github.com/pyflink/playgrounds) with modifications to support Flink 14.4, as well as minor cosmetic changes.  
**[Table API, UDF, Kafka, Elasticsearch, Kibana]**

[(4)](stateful-pair-avg) Example of combining the table and datastream API's to implement the keyed state function from [here](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/fault-tolerance/state/) is included.    
**[Table API, Datastream API, Stateful Function, Kafka, Elasticsearch, Kibana]**

[(5)](stateful-longest-run) Furthermore, a different keyed state function, analyses random 1's and 0's generated and sent with kafka. The flink job records the longest run of consecutive 1's and 0's and when it occurs is included. A kibana dashboard displaying the results is also included.        
**[Table API, Datastream API, Stateful Function, Kafka, Elasticsearch, Kibana]**

*It may be wise to remove and rebuild the images for (3) and (4) when switching between them.*
<!---
 (5) A process where integers are being generated and then analysed for their primality is also included.   
**[Table API, Datastream API, Stateful Function, Parallel Execution, Kafka, Elasticsearch, Kibana]**
-->
