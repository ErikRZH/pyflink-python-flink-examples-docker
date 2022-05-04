# Flink #

Flink is a framework for [distributed](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/overview/) stream (and batch) processing. You interact with Flink through one of its API's.

Flink has two main API's the [**Table API**](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/intro_to_table_api/) and the [**Datastream API**](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/datastream/intro_to_datastream_api/). Their relationship is illustrated in Fig. 1 below, taken from [here](https://www.youtube.com/watch?v=vLLn5PxF2Lw).

![alt text](images/Flink%20Architechture.PNG)

A Flink job requires two things, a **source** (an entry point for streaming/batch data) and a **sink** (an exit point for the streaming /batch data).

Between the source and the sink you probably want some more or less non-trivial **transformation on your data**.
To accomplish this most Flink programs follow the flow:
1. Obtain an execution environment,
2. Load/create the initial data, (**source**)
3. Specify transformations on this data, 
4. Specify where to put the results of your computations, (**sink**)
5. Trigger the program execution

Step 2. is where you use an API to specify the desired transformation. 

Flink features a graphical dashboard which can be used to see and manage running and completed jobs.

Flink is written in **Scala** and **Java**, however jobs can also be submitted using **Python**. This is thanks to the Python API ([**pyflink**](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/overview/)).
###Python + Flink = Pyflink ##
Using pyflink you can submit python code to be executed as Flink jobs. Pyflink supports both the Datastream and Table API's, simple examples of using pyflink can be found [here](https://github.com/apache/flink/tree/release-1.14/flink-python/pyflink/examples).

Pyflink's **Table API**  allows users to create and utilise **User Defined Functions ([UDF's](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/udfs/overview/))**, allowing you to use python, including python libraries such as pandas, to perform operations on the data.

### A Tale of Two APIs: ###
In the **Table API** the data is abstracted as a [dynamic table](https://nightlies.apache.org/flink/flink-docs-release-stable/docs/dev/table/concepts/dynamic_tables/#dynamic-tables-amp-continuous-queries) (the Pyflink `TableEnvironment` handles this, it is used for creating tables, submitting jobs etc. ) which you can manipilate as you would a table. Using python dynamic tables can be manipulated in the same way as a pandas table. The relational planner and rest of the Flink stack takes care of translating and optimising the operations described on the Table abstraction into streaming or batch operations, including managing state when required. For example in summation operations (an aggregate operation) etc. *Sources and sinks can be defined using either **SQL** or **python***

The **Datastream API** allows you more control over how the programme is executed, it does not feature the Table abstraction. In the DatastreamAPI the ``StreamExecutionEnvironment`` is what handles the abstraction, it is used to create datastreams and execute jobs etc. However, this API does not appear to support manipulation using Pandas etc. since it does not abstract the data as a table. 

It is possible to [translate between](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/datastream/intro_to_datastream_api/#conversion-between-datastream-and-table) the different APIs and you may therefore combine them in a symbiotic fashion.

## What now?
With this birds eye overview of Flink I would suggest looking at the examples, these include:
* The *word count example*, loads data from storage and counts the number of occurrences of each word, writes output to standard out.
* The *docker word count example* to see how docker can be used to produce the same result.
* The *pyflink-demo1* example, this is a more involved example featuring a metric generator connected to a kafka message broker which generate continous data. This data is then processed and stored using elasticseach, it can be visualised with Kibana.

When implementing custom stateful functions you have to be somewhat careful, this is since to allow Flink to handle states cleverly you have to declare states, and states are tied to keys in the data (so they may be backed up etc.). The more involved example will feature a stateful function which combines both API's.
[This example is illuminating for working with states](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/).

### Issues encountered
1. **Kafka/ES sinks and sources: connector issues due to changed interfaces in Flink 1.14**. Example 2 originally used Flink 1.13 and the connectors needed to interface with kafka and elasticsearch caused much trouble when upgrading to Flink 1.14. Given the evidence scattered online the following sequence of events seem to have cause the problem: in Flink 1.12 [a new kafka interface was introduced](https://nightlies.apache.org/flink/flink-docs-release-1.12/release-notes/flink-1.12.html) for connectors, and in [Flink 1.14](https://nightlies.apache.org/flink/flink-docs-release-1.14/release-notes/flink-1.14/) they [discontinued the old format and changed all connector interfaces](https://issues.apache.org/jira/browse/FLINK-23513). Thus: if you try to use connectors but get an error along the lines of `pyflink.util.exceptions.TableException: org.apache.flink.table.api.TableException: findAndCreateTableSource failed.` this may be what is causing you grief.
