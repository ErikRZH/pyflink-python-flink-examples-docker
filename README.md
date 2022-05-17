# Pyflink and Docker Examples
This repository concerns the Python interface of the Flink Streaming Processing framework, called pyflink.

While examples and documentation for using pyflink with docker may be found [here](https://nightlies.apache.org/flink/flink-docs-stable/), [here](https://github.com/apache/flink/tree/release-1.14/flink-python/pyflink/examples) and [here](https://github.com/apache/flink-playgrounds/tree/master/pyflink-walkthrough) (only for Flink 1.13 at the time of writing) some may benefit from seeing more examples, including examples of additional use cases. 

This repository is intended to provide such further examples, **the repository makes no claim to represent best Pyflink practices**.

This version uses **Flink 1.14** be aware that some functionality may differ between versions.
## Examples
[(1)](wordcount) The [standard](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table_api_tutorial/) wordcount example for streaming and batch using both the datastream and table APIs in a docker container.   
**[Table API, Datastream API]**

[(2)](modified-playground) A version of the playground from [here](https://github.com/apache/flink-playgrounds/tree/master/pyflink-walkthrough) with modifications to support Flink 14.4, as well as minor cosmetic changes.  
**[Table API, UDF, Kafka, Elasticsearch, Kibana]**

[(3)](stateful-pair-avg) Example of combining the table and datastream API's to implement the keyed state function from [here](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/fault-tolerance/state/) is included.    
**[Table API, Datastream API, Stateful Function, Kafka, Elasticsearch, Kibana]**

[(4)](stateful-longest-run) Another keyed state function, analysing random 1's and 0's generated and sent with kafka is included. The flink job records the longest run of consecutive 1's or 0's and the time it occurs. A kibana dashboard displaying the results is included.        
**[Table API, Datastream API, Stateful Function, Kafka, Elasticsearch, Kibana]**

*It may be wise to remove and rebuild the images for (3) and (4) when switching between them.*
<!---
 (5) A process where integers are being generated and then analysed for their primality is also included.   
**[Table API, Datastream API, Stateful Function, Parallel Execution, Kafka, Elasticsearch, Kibana]**
-->
